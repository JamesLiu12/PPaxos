import sys
import yaml

def change_conflict(c):
    """
    Update conflict settings in local.conf and evaluate/config.yaml
    
    Args:
        c: Input argument to calculate conflict value (c*10)
    """
    conflict_value = int(c) * 10
    
    # Update local.conf
    try:
        with open('./local.conf', 'r') as f:
            lines = f.readlines()
        
        # Find and replace the line starting with 'conflicts:'
        for i, line in enumerate(lines):
            if line.strip().startswith('conflicts:'):
                lines[i] = f'conflicts: {conflict_value}\n'
                break
        
        # Write back to local.conf
        with open('./local.conf', 'w') as f:
            f.writelines(lines)
        
        print(f"Updated local.conf - conflicts: {conflict_value}")
        
    except FileNotFoundError:
        print("Error: ./local.conf not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error updating local.conf: {e}")
        sys.exit(1)
    
    # Update evaluate/config.yaml
    try:
        with open('./evaluate/config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Update test_name
        config['test_name'] = f'conflict-{conflict_value}'
        
        # Write back to evaluate/config.yaml
        with open('./evaluate/config.yaml', 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        
        print(f"Updated evaluate/config.yaml - test_name: conflict-{conflict_value}")
        
    except FileNotFoundError:
        print("Error: ./evaluate/config.yaml not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error updating evaluate/config.yaml: {e}")
        sys.exit(1)

def main():
    if len(sys.argv) != 2:
        print("Usage: python change_conflict.py <c>")
        sys.exit(1)
    
    try:
        c = sys.argv[1]
        change_conflict(c)
    except ValueError:
        print("Error: Argument must be a number")
        sys.exit(1)

if __name__ == "__main__":
    main()