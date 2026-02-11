
import sys
import os
import re

# Add path to reach the pipeline dict
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from pipeline.standardization import PARENT_CHILD_MAPPING

def validate_mapping():
    print("🚀 VALIDATING PARENT_CHILD_MAPPING...")
    
    seen_children = {}
    duplicates = []
    
    for parent, children in PARENT_CHILD_MAPPING.items():
        if not isinstance(children, list):
            print(f"❌ Error: Parent '{parent}' has invalid children type: {type(children)}")
            continue
            
        for child in children:
            # Normalize for checking
            norm_child = re.sub(r'\s+', ' ', str(child)).strip().lower()
            
            if norm_child in seen_children:
                existing_parent = seen_children[norm_child]
                # Only flag if parents are different (same parent dupes are just cleanup)
                if existing_parent != parent:
                    duplicates.append({
                        'child': child,
                        'parent_1': existing_parent,
                        'parent_2': parent
                    })
            else:
                seen_children[norm_child] = parent

    if duplicates:
        print(f"\n❌ FOUND {len(duplicates)} CONFLICTING MAPPINGS:")
        for d in duplicates:
            print(f"   - Child '{d['child']}' is mapped to BOTH:")
            print(f"     1. {d['parent_1']}")
            print(f"     2. {d['parent_2']}")
    else:
        print("\n✅ VALIDATION PASSED! No conflicting child mappings found.")
        print(f"   - Total unique parents: {len(PARENT_CHILD_MAPPING)}")

if __name__ == "__main__":
    validate_mapping()
