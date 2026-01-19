<?php

ini_set('memory_limit', '1024M');
set_time_limit(0);

/* ---------------- ENV ---------------- */

define('FTP_HOST', getenv('FTP_HOST'));
define('FTP_USER', getenv('FTP_USER'));
define('FTP_PASS', getenv('FTP_PASS'));
define('FTP_BASE_DIR', getenv('FTP_BASE_DIR'));
define('CURR_URL', rtrim(getenv('CURR_URL'), '/'));

define('SITEMAP_OFFSET', (int)(getenv('SITEMAP_OFFSET') ?: 0));
define('MAX_SITEMAPS', (int)(getenv('MAX_SITEMAPS') ?: 0));
define('MAX_URLS_PER_SITEMAP', (int)(getenv('MAX_URLS_PER_SITEMAP') ?: 0));

define('SITEMAP_INDEX', CURR_URL . '/sitemap.xml');
define('OUTPUT_CSV', 'products_chunk.csv');

/* ---------------- LOGGER ---------------- */

function logMsg(string $msg): void
{
    echo '[' . date('H:i:s') . '] ' . $msg . PHP_EOL;
    flush();
}

/* ---------------- FTP ---------------- */

function uploadToFtp(string $file): void
{
    logMsg("Uploading CSV to FTP: {$file}");

    $conn = ftp_connect(FTP_HOST, 21, 30);
    ftp_login($conn, FTP_USER, FTP_PASS);
    ftp_pasv($conn, true);

    ensureFtpDir($conn, FTP_BASE_DIR);
    ftp_chdir($conn, FTP_BASE_DIR);
    ftp_put($conn, basename($file), $file, FTP_BINARY);

    ftp_close($conn);
    logMsg("FTP upload completed");
}

function ensureFtpDir($conn, string $path): void
{
    foreach (explode('/', trim($path, '/')) as $dir) {
        if (!@ftp_chdir($conn, $dir)) {
            ftp_mkdir($conn, $dir);
            ftp_chdir($conn, $dir);
        }
    }
}

/* ---------------- HTTP ---------------- */

function httpGet(string $url): ?string
{
    return @file_get_contents($url, false, stream_context_create([
        'http' => [
            'timeout' => 30,
            'user_agent' => 'EE-Scraper/1.0'
        ]
    ])) ?: null;
}

function loadXml(string $url): ?SimpleXMLElement
{
    $xml = httpGet($url);
    return $xml ? simplexml_load_string($xml) : null;
}

function fetchJson(string $url): ?array
{
    $json = httpGet($url);
    return $json ? json_decode($json, true) : null;
}

function normalizeImage(string $url): string
{
    return str_starts_with($url, '//') ? 'https:' . $url : $url;
}

/* ---------------- PRODUCT ---------------- */

function processProduct(
    string $url,
    $csv,
    array &$seen,
    int $urlIndex,
    int $totalUrls
): void {
    if (isset($seen[$url])) return;
    $seen[$url] = true;

    logMsg("  → Product [$urlIndex/$totalUrls] $url");

    $product = fetchJson(rtrim($url, '/') . '.js');
    if (!$product || empty($product['variants'])) {
        logMsg("    ⚠️  Invalid product JSON");
        return;
    }

    $options = $product['options'] ?? [];
    $images  = implode(',', array_map('normalizeImage', $product['images'] ?? []));

    foreach ($product['variants'] as $v) {
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
            rtrim($url, '/') . '?variant=' . $v['id'],
            $images
        ]);
    }

    logMsg("    ✓ Variants written: " . count($product['variants']));
    usleep(150000); // throttle
}

/* ---------------- MAIN ---------------- */

logMsg("Scraper started");
logMsg("Base URL: " . CURR_URL);
logMsg("Sitemap offset: " . SITEMAP_OFFSET);
logMsg("Max sitemaps: " . (MAX_SITEMAPS ?: 'ALL'));
logMsg("Max URLs per sitemap: " . (MAX_URLS_PER_SITEMAP ?: 'ALL'));

$index = loadXml(SITEMAP_INDEX);
if (!$index) {
    logMsg("❌ Failed to load sitemap index");
    exit(1);
}

$index->registerXPathNamespace('ns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
$sitemaps = $index->xpath('//ns:sitemap/ns:loc') ?: [];

$totalSitemaps = count($sitemaps);
$sitemaps = array_slice(
    $sitemaps,
    SITEMAP_OFFSET,
    MAX_SITEMAPS > 0 ? MAX_SITEMAPS : null
);

logMsg("Total sitemaps found: $totalSitemaps");
logMsg("Processing sitemaps: " . count($sitemaps));

$csv = fopen(OUTPUT_CSV, 'w');
fputcsv($csv, [
    'product_id','product_title','vendor','type','handle',
    'variant_id','variant_title','sku',
    'option_1_name','option_1_value',
    'option_2_name','option_2_value',
    'option_3_name','option_3_value',
    'variant_price','available','variant_url','image_url'
]);

$seen = [];
$sitemapIndex = 0;

foreach ($sitemaps as $map) {
    $sitemapIndex++;
    logMsg("▶ Sitemap [$sitemapIndex/" . count($sitemaps) . "] $map");

    $xml = loadXml((string)$map);
    if (!$xml) {
        logMsg("  ⚠️ Failed to load sitemap");
        continue;
    }

    $ns = $xml->getNamespaces(true);
    $xml->registerXPathNamespace('ns', $ns[''] ?? '');

    $urls = $xml->xpath('//ns:url/ns:loc') ?: [];

    if (MAX_URLS_PER_SITEMAP > 0) {
        $urls = array_slice($urls, 0, MAX_URLS_PER_SITEMAP);
    }

    logMsg("  URLs found: " . count($urls));

    $urlIndex = 0;
    foreach ($urls as $loc) {
        $urlIndex++;
        processProduct((string)$loc, $csv, $seen, $urlIndex, count($urls));
    }
}

fclose($csv);
logMsg("CSV generation completed");

uploadToFtp(OUTPUT_CSV);
logMsg("Scraper finished successfully");