import csv
import os
import glob
from typing import List, Dict, Set
from pathlib import Path

class FolderFileMerger:
    def __init__(self, base_folder_path: str, target_folder_name: str = "all_file"):
        """
        Initialize the merger with the base folder path
        
        Args:
            base_folder_path: Path where your code file is located
            target_folder_name: Name of the folder containing multiple subfolders (default: "all_file")
        """
        self.base_folder = base_folder_path
        self.target_folder = os.path.join(base_folder_path, target_folder_name)
        
        print(f"Base folder (code location): {self.base_folder}")
        print(f"Target folder (all_file): {self.target_folder}")
        
        # Define file patterns and their mappings for INPUT FILES
        self.file_patterns = {
            'approve_mark_products': {
                'pattern': '*[aA][pP][pP][rR][oO][vV][eE]_[mM][aA][rR][kK]_[pP][rR][oO][dD][uU][cC][tT][sS]*.csv',
                'type': 'approve_match',
                'description': 'Approve Mark Products Files',
                'output': 'approve_merge'  # Maps to which output file
            },
            'new_update_matches': {
                'pattern': '*[nN][eE][wW]_[uU][pP][dD][aA][tT][eE]_[mM][aA][tT][cC][hH][eE][sS]*.csv',
                'type': 'new_match',
                'description': 'New Update Matches Files',
                'output': 'new_merge'  # Maps to which output file
            },
            'wrong_no_replacement': {
                'pattern': '*[wW][rR][oO][nN][gG]_[nN][oO]_[rR][eE][pP][lL][aA][cC][eE][mM][eE][nN][tT]*.csv',
                'type': 'wrong_match_source',
                'description': 'Wrong No Replacement Files',
                'output': 'wrongmatch_merge'  # Maps to which output file
            }
        }
        
        # Define output file configurations for OUTPUT FILES
        self.output_files = {
            'wrongmatch_merge': {
                'csv': 'wrongmatch_merge.csv',
                'headers': ['repricer_id','product_id', 'competitor_id', 'type', 'source', 'is_issue', 'reject_reason', 'is_issue_details', 'reviewed_by_user']
            },
            'approve_merge': {
                'csv': 'approve_merge.csv',
                'headers': ['product_id', 'competitor_id', 'type', 'source', 'is_issue', 'reject_reason', 'is_issue_details', 'reviewed_by_user']
            },
            'new_merge': {
                'csv': 'new_merge.csv',
                'headers': ['sku', 'ref_sku', 'ref_url', 'ref_name', 'send_in_feed']
            }
        }
        
        # Initialize merged data storage with OUTPUT FILE names
        self.merged_data = {
            'wrongmatch_merge': [],
            'approve_merge': [],
            'new_merge': []
        }
        
        # Store raw data from source files for conditional processing
        self.source_data = {
            'wrong_no_replacement': [],  # Store to check for "Wrong Match" reason
            'new_update_matches': []     # Store to check for "Brand mismatch overridden by exact key" reason
        }
        
        # Track statistics for INPUT FILES
        self.stats = {
            'approve_mark_products': {'files': 0, 'records': 0, 'folders': set()},
            'new_update_matches': {'files': 0, 'records': 0, 'folders': set()},
            'wrong_no_replacement': {'files': 0, 'records': 0, 'folders': set()}
        }

    def check_folder_structure(self) -> bool:
        """Check if the required folder structure exists"""
        if not os.path.exists(self.target_folder):
            print(f"❌ Error: '{self.target_folder}' folder not found!")
            print(f"Please make sure there is a folder named 'all_file' in: {self.base_folder}")
            return False
        
        if not os.path.isdir(self.target_folder):
            print(f"❌ Error: '{self.target_folder}' is not a directory!")
            return False
        
        return True

    def find_all_subfolders(self) -> List[str]:
        """Find all subfolders inside the all_file folder"""
        try:
            subfolders = [f.path for f in os.scandir(self.target_folder) if f.is_dir()]
            subfolders.sort()
            
            print(f"\nFound {len(subfolders)} subfolders in {os.path.basename(self.target_folder)}:")
            for i, folder in enumerate(subfolders, 1):
                print(f"  {i}. {os.path.basename(folder)}")
            
            return subfolders
        except Exception as e:
            print(f"Error finding subfolders: {e}")
            return []

    def find_files_in_folder(self, folder_path: str) -> Dict[str, List[str]]:
        """Find all relevant files in a folder"""
        files_found = {pattern: [] for pattern in self.file_patterns.keys()}
        
        try:
            folder_name = os.path.basename(folder_path)
            
            # Search for each file pattern
            for file_key, file_info in self.file_patterns.items():
                pattern = file_info['pattern']
                found_files = glob.glob(os.path.join(folder_path, pattern))
                files_found[file_key] = found_files
                
                if found_files:
                    print(f"    Found {len(found_files)} {file_info['description']}")
                    for file in found_files:
                        print(f"      - {os.path.basename(file)}")
            
        except Exception as e:
            print(f"Error finding files in {folder_path}: {e}")
        
        return files_found

    def read_csv_file(self, file_path: str) -> List[Dict]:
        """Read a CSV/TSV file and return list of dictionaries"""
        data = []
        try:
            # Detect delimiter
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                sample = f.read(1024)
                delimiter = '\t' if '\t' in sample else ','
            
            # Read file
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                for row in reader:
                    # Clean up the row
                    clean_row = {k.strip(): (v.strip() if v else '') for k, v in row.items() if k}
                    data.append(clean_row)
            
            print(f"    ✅ Read {len(data)} records from {os.path.basename(file_path)}")
        except Exception as e:
            print(f"    ❌ Error reading CSV file {file_path}: {e}")
        
        return data

    def process_wrong_no_replacement(self, records: List[Dict], folder_name: str):
        """Process wrong_no_replacement files for wrongmatch_merge with condition"""
        # First store for exclusion checking
        folder_records_count = len(records)
        
        for record in records:
            self.source_data['wrong_no_replacement'].append({
                'product_id': record.get('product_id', ''),
                'competitor_id': record.get('competitor_id', ''),
                'cm_reason': record.get('cm_reason', '').strip().lower()
            })
        
        # Create a set of product_ids that have "Wrong Match" reason in wrong_no_replacement
        excluded_products = set()
        for item in self.source_data['wrong_no_replacement']:
            if 'wrong match' in item['cm_reason']:
                excluded_products.add(f"{item['product_id']}_{item['competitor_id']}")
        
        # Process records for wrongmatch_merge
        skipped_count = 0
        for record in records:
            product_id = record.get('product_id', '')
            competitor_id = record.get('competitor_id', '')
            key = f"{product_id}_{competitor_id}"
            
            # Skip if this product has "Wrong Match" reason
            if key in excluded_products:
                skipped_count += 1
                continue
            
            # Create wrongmatch_merge record with blank is_issue_details and reviewed_by_user
            wrong_record = {
                'product_id': product_id,
                'competitor_id': competitor_id,
                'type': 'update',
                'source': 'CM',
                'is_issue': 'Rejected',
                'reject_reason': 'Auto Validated',
                'is_issue_details': '',  # Set to blank
                'reviewed_by_user': ''    # Set to blank
            }
            self.merged_data['wrongmatch_merge'].append(wrong_record)
        
        # Calculate net records
        net_records = folder_records_count - skipped_count
        
        # Print summary for this folder with total and skipped count
        if skipped_count > 0:
            print(f"      Total {folder_records_count} records, Skipped {skipped_count} products with 'Wrong Match' reason in {folder_name}, Net {net_records} records for wrongmatch_merge")
        else:
            print(f"      Total {folder_records_count} records, No products skipped in {folder_name}")

    def process_approve_mark_products(self, records: List[Dict], folder_name: str):
        """Process approve_mark_products files for approve_merge"""
        for record in records:
            approve_record = {
                'product_id': record.get('product_id', ''),
                'competitor_id': record.get('competitor_id', ''),
                'type': 'update',
                'source': 'CM',
                'is_issue': 'Approved',
                'reject_reason': '',
                'is_issue_details': record.get('cm_reason', record.get('existing_reason', '')),
                'reviewed_by_user': record.get('reviewed_by_user', 'system')
            }
            self.merged_data['approve_merge'].append(approve_record)

    def process_new_update_matches(self, records: List[Dict], folder_name: str):
        """Process new_update_matches files for new_merge with condition to exclude if remark = 'Brand mismatch overridden by exact key'"""
        folder_records_count = len(records)
        
        # Store for statistics
        for record in records:
            self.source_data['new_update_matches'].append({
                'sku': record.get('sku', ''),
                'remark': record.get('remark', '').strip().lower()
            })
        
        # Process records for new_merge
        skipped_count = 0
        for record in records:
            remark = record.get('remark', '').strip().lower()
            
            # Skip if remark contains "Brand mismatch overridden by exact key"
            if 'brand mismatch overridden by exact key' in remark:
                skipped_count += 1
                continue
            
            new_record = {
                'sku': record.get('sku', ''),
                'ref_sku': record.get('ref_sku', ''),
                'ref_url': record.get('ref_url', ''),
                'ref_name': record.get('ref_name', ''),
                'send_in_feed': 1
            }
            # Only add if sku exists
            if new_record['sku']:
                self.merged_data['new_merge'].append(new_record)
        
        # Calculate net records
        net_records = folder_records_count - skipped_count
        
        # Print summary for this folder with total and skipped count
        if skipped_count > 0:
            print(f"      Total {folder_records_count} records, Skipped {skipped_count} products with 'Brand mismatch overridden by exact key' reason in {folder_name}, Net {net_records} records for new_merge")
        else:
            print(f"      Total {folder_records_count} records, No products skipped in {folder_name}")

    def process_files(self, folder_name: str, files_found: Dict[str, List[str]]):
        """Process all files found in a folder"""
        
        # Process approve_mark_products files for approve_merge
        if files_found['approve_mark_products']:
            for file_path in files_found['approve_mark_products']:
                records = self.read_csv_file(file_path)
                if records:
                    self.process_approve_mark_products(records, folder_name)
                    self.stats['approve_mark_products']['files'] += 1
                    self.stats['approve_mark_products']['records'] += len(records)
                    self.stats['approve_mark_products']['folders'].add(folder_name)
        
        # Process new_update_matches files for new_merge
        if files_found['new_update_matches']:
            for file_path in files_found['new_update_matches']:
                records = self.read_csv_file(file_path)
                if records:
                    self.process_new_update_matches(records, folder_name)
                    self.stats['new_update_matches']['files'] += 1
                    self.stats['new_update_matches']['records'] += len(records)
                    self.stats['new_update_matches']['folders'].add(folder_name)
        
        # Process wrong_no_replacement files for wrongmatch_merge with condition
        if files_found['wrong_no_replacement']:
            for file_path in files_found['wrong_no_replacement']:
                records = self.read_csv_file(file_path)
                if records:
                    self.process_wrong_no_replacement(records, folder_name)
                    self.stats['wrong_no_replacement']['files'] += 1
                    self.stats['wrong_no_replacement']['records'] += len(records)
                    self.stats['wrong_no_replacement']['folders'].add(folder_name)

    def process_all_folders(self):
        """Process all subfolders and merge files"""
        if not self.check_folder_structure():
            return
        
        subfolders = self.find_all_subfolders()
        
        if not subfolders:
            print(f"\n⚠️ No subfolders found inside '{os.path.basename(self.target_folder)}'!")
            return
        
        print("\n" + "="*80)
        print("PROCESSING FOLDERS")
        print("="*80)
        
        for folder_index, folder_path in enumerate(subfolders, 1):
            folder_name = os.path.basename(folder_path)
            print(f"\n📁 Folder {folder_index}: {folder_name}")
            print("-" * 60)
            
            files_found = self.find_files_in_folder(folder_path)
            self.process_files(folder_name, files_found)

    def save_merged_files(self):
        """Save all merged data to output files"""
        print("\n" + "="*80)
        print("SAVING MERGED FILES")
        print("="*80)
        print(f"Saving to: {self.base_folder}")
        
        for output_key, output_info in self.output_files.items():
            data = self.merged_data[output_key]
            headers = output_info['headers']
            csv_output = os.path.join(self.base_folder, output_info['csv'])
            
            if data:
                self.save_to_csv(data, csv_output, headers)
                
                print(f"\n📊 {output_key.upper()} Summary:")
                print(f"  Total records: {len(data)}")
                print(f"  CSV: {os.path.basename(csv_output)}")
            else:
                self.create_empty_file(csv_output, headers)
                print(f"\n⚠️ No data for {output_key} - created empty files")

    def save_to_csv(self, data: List[Dict], output_file: str, headers: List[str]):
        """Save data to CSV file with fixed headers"""
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                for record in data:
                    clean_record = {header: record.get(header, '') for header in headers}
                    writer.writerow(clean_record)
            print(f"  ✅ Saved {len(data)} records to {os.path.basename(output_file)}")
        except Exception as e:
            print(f"  ❌ Error saving to CSV {output_file}: {e}")
    
    def create_empty_file(self, output_file: str, headers: List[str]):
        """Create an empty CSV file with headers"""
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
            print(f"  📄 Created empty file: {os.path.basename(output_file)}")
        except Exception as e:
            print(f"  ❌ Error creating empty file {output_file}: {e}")

    def print_summary(self):
        """Print final summary of merged data"""
        print("\n" + "="*80)
        print("FINAL MERGE SUMMARY")
        print("="*80)
        
        # Calculate skipped products count for wrong_no_replacement
        wrong_skipped_count = 0
        for item in self.source_data['wrong_no_replacement']:
            if 'wrong match' in item['cm_reason']:
                wrong_skipped_count += 1
        
        # Calculate skipped products count for new_update_matches
        new_skipped_count = 0
        for item in self.source_data['new_update_matches']:
            if 'brand mismatch overridden by exact key' in item['remark']:
                new_skipped_count += 1
        
        # Calculate net records after skipping
        net_wrong_records = self.stats['wrong_no_replacement']['records'] - wrong_skipped_count
        net_new_records = self.stats['new_update_matches']['records'] - new_skipped_count
        
        # Calculate unique product IDs for each output file
        unique_approve_products = set()
        unique_wrong_products = set()
        unique_new_skus = set()
        
        for record in self.merged_data['approve_merge']:
            if record.get('product_id'):
                unique_approve_products.add(record['product_id'])
        
        for record in self.merged_data['wrongmatch_merge']:
            if record.get('product_id'):
                unique_wrong_products.add(record['product_id'])
        
        for record in self.merged_data['new_merge']:
            if record.get('sku'):
                unique_new_skus.add(record['sku'])
        
        print("\n📊 INPUT FILES PROCESSED ACROSS ALL FOLDERS:")
        print("-" * 60)
        print(f"  approve_mark_products.csv  → {self.stats['approve_mark_products']['records']} total records from {self.stats['approve_mark_products']['files']} files in {len(self.stats['approve_mark_products']['folders'])} folders")
        print(f"  new_update_matches.csv     → {self.stats['new_update_matches']['records']} total records from {self.stats['new_update_matches']['files']} files in {len(self.stats['new_update_matches']['folders'])} folders")
        print(f"      └─ After removing {new_skipped_count} skipped products with 'Brand mismatch overridden by exact key': {net_new_records} net records")
        print(f"  wrong_no_replacement.csv   → {self.stats['wrong_no_replacement']['records']} total records from {self.stats['wrong_no_replacement']['files']} files in {len(self.stats['wrong_no_replacement']['folders'])} folders")
        print(f"      └─ After removing {wrong_skipped_count} skipped products with 'Wrong Match': {net_wrong_records} net records")
        
        print("\n📊 SKIPPED PRODUCTS SUMMARY:")
        print("-" * 60)
        print(f"  ⏭️  Products skipped from wrong_no_replacement.csv (Wrong Match reason): {wrong_skipped_count}")
        print(f"  ⏭️  Products skipped from new_update_matches.csv (Brand mismatch overridden by exact key): {new_skipped_count}")
        
        print("\n📊 OUTPUT FILES CREATED:")
        print("-" * 60)
        print(f"  approve_merge.csv: {len(self.merged_data['approve_merge'])} records ({len(unique_approve_products)} unique product IDs)")
        print(f"  wrongmatch_merge.csv: {len(self.merged_data['wrongmatch_merge'])} records ({len(unique_wrong_products)} unique product IDs)")
        print(f"  new_merge.csv: {len(self.merged_data['new_merge'])} records ({len(unique_new_skus)} unique SKUs)")
        
        print("\n" + "="*80)
        print("✅ MERGE COMPLETED SUCCESSFULLY")
        print("="*80)

def main():
    """Main function to run the folder merger"""
    print("="*80)
    print("FOLDER FILE MERGER WITH CONDITIONS")
    print("="*80)
    
    script_location = os.path.dirname(os.path.abspath(__file__))
    
    print(f"\n📁 Script location: {script_location}")
    print("\n🔍 File Mappings:")
    print("-" * 60)
    print("INPUT FILES → → → → → → OUTPUT FILES")
    print("-" * 60)
    print("approve_mark_products.csv  → → approve_merge.csv")
    print("new_update_matches.csv     → → new_merge.csv (excludes if remark='Brand mismatch overridden by exact key')")
    print("wrong_no_replacement.csv   → → wrongmatch_merge.csv (excludes if reason='Wrong Match')")
    print("-" * 60)
    print("\n📄 OUTPUT FILES:")
    print("  - approve_merge.csv")
    print("  - new_merge.csv")
    print("  - wrongmatch_merge.csv")
    print("="*60)
    
    merger = FolderFileMerger(script_location, "all_file")
    merger.process_all_folders()
    merger.save_merged_files()
    merger.print_summary()

if __name__ == "__main__":
    main()