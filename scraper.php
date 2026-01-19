<?php
// scraper.php

ini_set('memory_limit', '2048M');
set_time_limit(0);

/* ---------------- ENV ---------------- */

$FTP_HOST     = getenv('FTP_HOST') ?: '';
$FTP_USER     = getenv('FTP_USER') ?: '';
$FTP_PASS     = getenv('FTP_PASS') ?: '';
$FTP_BASE_DIR = getenv('FTP_BASE_DIR') ?: '';
$CURR_URL     = rtrim(getenv('CURR_URL') ?: '', '/');

$BATCH_ID     = intval(getenv('BATCH_ID') ?: 1);
$BATCH_SIZE   = intval(getenv('BATCH_SIZE') ?: 5000);
$BATCH_START  = intval(getenv('BATCH_START') ?: 0);
$CONCURRENCY  = intval(getenv('CONCURRENCY') ?: 15);

if (!$CURR_URL) die('CURR_URL missing');

define('SITEMAP_INDEX', $CURR_URL . '/sitemap.xml');
define('OUTPUT_CSV', 'products_part_' . $BATCH_ID . '.csv');

/* ---------------- FTP ---------------- */

function uploadToFtp($file, $host, $user, $pass, $dir)
{
    if (!$host || !$user || !$pass || !$dir) return;

    $conn = ftp_connect($host, 21, 30);
    if (!$conn || !ftp_login($conn, $user, $pass)) return;

    ftp_pasv($conn, true);
    foreach (explode('/', trim($dir, '/')) as $d) {
        if (!@ftp_chdir($conn, $d)) {
            ftp_mkdir($conn, $d);
            ftp_chdir($conn, $d);
        }
    }

    ftp_put($conn, basename($file), $file, FTP_BINARY);
    ftp_close($conn);
}

/* ---------------- HTTP ---------------- */

function normalizeImage($url)
{
    return strpos($url, '//') === 0 ? 'https:' . $url : $url;
}

function curlCreate($url)
{
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_TIMEOUT        => 30,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_USERAGENT      => 'EE-Scraper/1.0'
    ]);
    return $ch;
}

/* ---------------- LOAD SITEMAPS ---------------- */

$index = simplexml_load_string(file_get_contents(SITEMAP_INDEX));
$index->registerXPathNamespace('ns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
$sitemaps = $index->xpath('//ns:sitemap/ns:loc');

/* ---------------- LOAD PRODUCT URLS (BATCHED) ---------------- */

$productUrls = [];
$counter = 0;

foreach ($sitemaps as $map) {
    $xml = simplexml_load_string(file_get_contents((string)$map));
    $ns  = $xml->getNamespaces(true);
    $xml->registerXPathNamespace('ns', $ns[''] ?? '');

    foreach ($xml->xpath('//ns:url/ns:loc') as $loc) {
        if ($counter++ < $BATCH_START) continue;
        if (count($productUrls) >= $BATCH_SIZE) break 2;

        $productUrls[] = rtrim((string)$loc, '/') . '.js';
    }
}

/* ---------------- CSV ---------------- */

$csv = fopen(OUTPUT_CSV, 'w');
fputcsv($csv, [
    'product_id','product_title','vendor','type','handle',
    'variant_id','variant_title','sku',
    'option_1_name','option_1_value',
    'option_2_name','option_2_value',
    'option_3_name','option_3_value',
    'variant_price','available','variant_url','image_url'
]);

/* ---------------- CURL MULTI ---------------- */

$mh = curl_multi_init();
$handles = [];
$pointer = 0;

function addHandle()
{
    global $productUrls, $pointer, $handles, $mh;
    if (!isset($productUrls[$pointer])) return;

    $ch = curlCreate($productUrls[$pointer]);
    $handles[(int)$ch] = $productUrls[$pointer];
    curl_multi_add_handle($mh, $ch);
    $pointer++;
}

for ($i = 0; $i < $GLOBALS['CONCURRENCY']; $i++) {
    addHandle();
}

do {
    curl_multi_exec($mh, $running);

    while ($info = curl_multi_info_read($mh)) {

        $ch  = $info['handle'];
        $url = $handles[(int)$ch] ?? '';
        $raw = curl_multi_getcontent($ch);

        unset($handles[(int)$ch]);
        curl_multi_remove_handle($mh, $ch);
        curl_close($ch);

        if ($raw && ($product = json_decode($raw, true))) {

            $baseUrl = str_replace('.js', '', $url);
            $options = $product['options'] ?? [];
            $images  = array_map('normalizeImage', $product['images'] ?? []);
            $images  = implode(',', $images);

            foreach ($product['variants'] ?? [] as $v) {
                fputcsv($csv, [
                    $product['id'],
                    $product['title'],
                    $product['vendor'],
                    $product['type'],
                    $product['handle'],
                    $v['id'],
                    $v['title'],
                    $v['sku'] ?? '',
                    $options[0]['name'] ?? '',
                    $v['option1'] ?? '',
                    $options[1]['name'] ?? '',
                    $v['option2'] ?? '',
                    $options[2]['name'] ?? '',
                    $v['option3'] ?? '',
                    $v['price'],
                    $v['available'] ? '1' : '0',
                    $baseUrl . '?variant=' . $v['id'],
                    $images
                ]);
            }
        }

        addHandle();
    }

    usleep(10000);
} while ($running || !empty($handles));

curl_multi_close($mh);
fclose($csv);

/* ---------------- FTP UPLOAD ---------------- */

uploadToFtp(OUTPUT_CSV, $FTP_HOST, $FTP_USER, $FTP_PASS, $FTP_BASE_DIR);

echo "Batch {$BATCH_ID} completed\n";