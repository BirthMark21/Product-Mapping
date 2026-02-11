#!/usr/bin/env python3
"""
Dynamic Mapping Manager
Manages product mappings dynamically without code changes
"""

import json
import os
import sys
import hashlib
import shutil
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from pipeline.standardization import PARENT_CHILD_MAPPING, _generate_stable_uuid

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MAPPING_FILE = Path(__file__).parent / 'mapping_config.json'
MAPPING_BACKUP_DIR = Path(__file__).parent / 'mapping_backups'


class DynamicMappingManager:
    """Manages dynamic product mappings"""
    
    def __init__(self):
        self.mapping_file = MAPPING_FILE
        self.backup_dir = MAPPING_BACKUP_DIR
        self.backup_dir.mkdir(exist_ok=True)
        self.mappings = self.load_mappings()
    
    def load_mappings(self) -> Dict:
        """Load mappings from JSON file"""
        if not self.mapping_file.exists():
            print(f"⚠️  Mapping file not found: {self.mapping_file}")
            return self._create_default_mappings()
        
        try:
            with open(self.mapping_file, 'r', encoding='utf-8') as f:
                mappings = json.load(f)
            print(f"✅ Loaded mappings from {self.mapping_file}")
            return mappings
        except Exception as e:
            print(f"❌ Failed to load mappings: {e}")
            return self._create_default_mappings()
    
    def _create_default_mappings(self) -> Dict:
        """Create default mapping structure"""
        
        mappings = {
            "version": "1.0",
            "last_updated": datetime.now().isoformat(),
            "description": "Dynamic product standardization mappings",
            "parent_products": {}
        }
        
        # Convert PARENT_CHILD_MAPPING to JSON format
        for parent_name, children in PARENT_CHILD_MAPPING.items():
            mappings["parent_products"][parent_name] = {
                "parent_id": _generate_stable_uuid(parent_name),
                "children": children
            }
        
        # Save default mappings
        self.save_mappings(mappings)
        return mappings
    
    def save_mappings(self, mappings: Dict = None):
        """Save mappings to JSON file"""
        if mappings is None:
            mappings = self.mappings
        
        # Create backup before saving
        self._create_backup()
        
        # Update timestamp
        mappings['last_updated'] = datetime.now().isoformat()
        
        try:
            with open(self.mapping_file, 'w', encoding='utf-8') as f:
                json.dump(mappings, f, indent=2, ensure_ascii=False)
            print(f"✅ Saved mappings to {self.mapping_file}")
            self.mappings = mappings
        except Exception as e:
            print(f"❌ Failed to save mappings: {e}")
            raise
    
    def _create_backup(self):
        """Create backup of current mappings"""
        if not self.mapping_file.exists():
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = self.backup_dir / f"mapping_config_{timestamp}.json"
        
        try:
            shutil.copy2(self.mapping_file, backup_file)
            print(f"📦 Created backup: {backup_file}")
        except Exception as e:
            print(f"⚠️  Failed to create backup: {e}")
    
    def add_parent_product(self, parent_name: str, children: List[str]):
        """Add a new parent product mapping"""
        
        if parent_name in self.mappings['parent_products']:
            print(f"⚠️  Parent product '{parent_name}' already exists")
            return False
        
        self.mappings['parent_products'][parent_name] = {
            "parent_id": _generate_stable_uuid(parent_name),
            "children": children
        }
        
        self.save_mappings()
        print(f"✅ Added parent product: {parent_name} with {len(children)} children")
        return True
    
    def add_child_to_parent(self, parent_name: str, child_name: str):
        """Add a child product to an existing parent"""
        if parent_name not in self.mappings['parent_products']:
            print(f"❌ Parent product '{parent_name}' not found")
            return False
        
        children = self.mappings['parent_products'][parent_name]['children']
        
        if child_name in children:
            print(f"⚠️  Child '{child_name}' already exists under '{parent_name}'")
            return False
        
        children.append(child_name)
        self.save_mappings()
        print(f"✅ Added child '{child_name}' to parent '{parent_name}'")
        return True
    
    def remove_child_from_parent(self, parent_name: str, child_name: str):
        """Remove a child product from a parent"""
        if parent_name not in self.mappings['parent_products']:
            print(f"❌ Parent product '{parent_name}' not found")
            return False
        
        children = self.mappings['parent_products'][parent_name]['children']
        
        if child_name not in children:
            print(f"⚠️  Child '{child_name}' not found under '{parent_name}'")
            return False
        
        children.remove(child_name)
        self.save_mappings()
        print(f"✅ Removed child '{child_name}' from parent '{parent_name}'")
        return True
    
    def rename_parent(self, old_name: str, new_name: str):
        """Rename a parent product"""
        if old_name not in self.mappings['parent_products']:
            print(f"❌ Parent product '{old_name}' not found")
            return False
        
        if new_name in self.mappings['parent_products']:
            print(f"❌ Parent product '{new_name}' already exists")
            return False
        
        # Copy to new name
        self.mappings['parent_products'][new_name] = self.mappings['parent_products'][old_name]
        
        # Update parent_id for new name
        self.mappings['parent_products'][new_name]['parent_id'] = _generate_stable_uuid(new_name)
        
        # Remove old name
        del self.mappings['parent_products'][old_name]
        
        self.save_mappings()
        print(f"✅ Renamed parent product from '{old_name}' to '{new_name}'")
        return True
    
    def get_parent_info(self, parent_name: str) -> Dict:
        """Get information about a parent product"""
        if parent_name not in self.mappings['parent_products']:
            return None
        return self.mappings['parent_products'][parent_name]
    
    def search_child(self, child_name: str) -> List[str]:
        """Find which parent(s) a child belongs to"""
        parents = []
        child_lower = child_name.lower().strip()
        
        for parent_name, data in self.mappings['parent_products'].items():
            for child in data['children']:
                if child.lower().strip() == child_lower:
                    parents.append(parent_name)
        
        return parents
    
    def get_statistics(self) -> Dict:
        """Get mapping statistics"""
        total_parents = len(self.mappings['parent_products'])
        total_children = sum(
            len(data['children']) 
            for data in self.mappings['parent_products'].values()
        )
        
        return {
            'total_parents': total_parents,
            'total_children': total_children,
            'last_updated': self.mappings.get('last_updated'),
            'version': self.mappings.get('version')
        }
    
    def validate_mappings(self) -> bool:
        """Validate mapping integrity"""
        print("\n🔍 VALIDATING MAPPINGS")
        print("=" * 60)
        
        issues = []
        
        # Check for duplicate children across parents
        all_children = {}
        for parent_name, data in self.mappings['parent_products'].items():
            for child in data['children']:
                child_lower = child.lower().strip()
                if child_lower in all_children:
                    issues.append(f"Duplicate child '{child}' in '{parent_name}' and '{all_children[child_lower]}'")
                else:
                    all_children[child_lower] = parent_name
        
        # Check for empty parent products
        for parent_name, data in self.mappings['parent_products'].items():
            if not data['children']:
                issues.append(f"Parent '{parent_name}' has no children")
        
        # Check for missing parent_id
        for parent_name, data in self.mappings['parent_products'].items():
            if 'parent_id' not in data:
                issues.append(f"Parent '{parent_name}' missing parent_id")
        
        if issues:
            print(f"❌ Found {len(issues)} issue(s):")
            for issue in issues:
                print(f"   • {issue}")
            return False
        else:
            print("✅ All mappings are valid")
            return True
    
    def export_to_python(self, output_file: str = None):
        """Export mappings to Python dictionary format"""
        if output_file is None:
            output_file = Path(__file__).parent.parent / 'pipeline' / 'standardization_mappings.py'
        
        # Generate Python code
        python_code = f'''"""
Auto-generated product mappings
Generated: {datetime.now().isoformat()}
DO NOT EDIT MANUALLY - Use dynamic_mapping_manager.py instead
"""

import uuid

NAMESPACE_UUID = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')

PARENT_CHILD_MAPPING = {{
'''
        
        for parent_name, data in sorted(self.mappings['parent_products'].items()):
            children_str = ', '.join([f'"{child}"' for child in data['children']])
            python_code += f'    "{parent_name}": [{children_str}],\n'
        
        python_code += '}\n'
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(python_code)
        
        print(f"✅ Exported mappings to {output_file}")
    
    def get_file_hash(self) -> str:
        """Get hash of current mapping file"""
        if not self.mapping_file.exists():
            return ""
        
        with open(self.mapping_file, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()


def main():
    """Interactive CLI for managing mappings"""
    
    parser = argparse.ArgumentParser(description='Dynamic Mapping Manager')
    parser.add_argument('--add-parent', nargs='+', help='Add parent product: --add-parent "Parent Name" "Child1" "Child2"')
    parser.add_argument('--add-child', nargs=2, help='Add child to parent: --add-child "Parent Name" "Child Name"')
    parser.add_argument('--remove-child', nargs=2, help='Remove child from parent: --remove-child "Parent Name" "Child Name"')
    parser.add_argument('--search', help='Search for child product')
    parser.add_argument('--stats', action='store_true', help='Show mapping statistics')
    parser.add_argument('--validate', action='store_true', help='Validate mappings')
    parser.add_argument('--export', action='store_true', help='Export to Python format')
    parser.add_argument('--init', action='store_true', help='Initialize default mappings')
    
    args = parser.parse_args()
    
    manager = DynamicMappingManager()
    
    if args.init:
        manager._create_default_mappings()
        print("✅ Initialized default mappings")
    
    elif args.add_parent:
        if len(args.add_parent) < 2:
            print("❌ Usage: --add-parent \"Parent Name\" \"Child1\" \"Child2\" ...")
        else:
            parent_name = args.add_parent[0]
            children = args.add_parent[1:]
            manager.add_parent_product(parent_name, children)
    
    elif args.add_child:
        manager.add_child_to_parent(args.add_child[0], args.add_child[1])
    
    elif args.remove_child:
        manager.remove_child_from_parent(args.remove_child[0], args.remove_child[1])
    
    elif args.search:
        parents = manager.search_child(args.search)
        if parents:
            print(f"✅ Found '{args.search}' under parent(s): {', '.join(parents)}")
        else:
            print(f"❌ '{args.search}' not found in any parent")
    
    elif args.stats:
        stats = manager.get_statistics()
        print("\n📊 MAPPING STATISTICS")
        print("=" * 60)
        print(f"Total parent products: {stats['total_parents']}")
        print(f"Total child products: {stats['total_children']}")
        print(f"Last updated: {stats['last_updated']}")
        print(f"Version: {stats['version']}")
    
    elif args.validate:
        manager.validate_mappings()
    
    elif args.export:
        manager.export_to_python()
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
