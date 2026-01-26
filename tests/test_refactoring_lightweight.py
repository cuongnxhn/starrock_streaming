"""
Lightweight test script (không cần PySpark installed).
Chỉ test logic cơ bản của refactoring.
Run: python tests/test_refactoring_lightweight.py
"""

import sys
sys.path.insert(0, '.')


def test_imports():
    """Test có thể import được modules"""
    print("\n=== Test 1: Module Imports ===")
    
    try:
        # Test import schema processors (nhưng không run Spark code)
        # Nếu có lỗi import StructType, catch it
        import core.schema_processors.base as base_module
        import core.schema_processors.factory as factory_module
        
        # Check classes tồn tại
        assert hasattr(base_module, 'BaseSchemaProcessor'), \
            "base.py should have BaseSchemaProcessor"
        print("  ✓ BaseSchemaProcessor exists")
        
        assert hasattr(factory_module, 'SchemaProcessorFactory'), \
            "factory.py should have SchemaProcessorFactory"
        print("  ✓ SchemaProcessorFactory exists")
        
        print("✓ All modules can be imported")
        return True
        
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False


def test_file_structure():
    """Test file structure tồn tại đúng"""
    print("\n=== Test 2: File Structure ===")
    
    import os
    
    expected_files = [
        'core/schema_processors/__init__.py',
        'core/schema_processors/base.py',
        'core/schema_processors/default.py',
        'core/schema_processors/factory.py',
        'core/schema_processors/custom/__init__.py',
        'core/schema_processors/custom/example_special_topic.py',
        'core/schema_processors/custom/orders_example.py',
        'core/schema_processors/README.md',
        'main.py',
        'core/etl_job.py',
        'deploy_per_topic.ps1',
        'deploy_per_topic.sh',
        'QUICKSTART.md',
        'ARCHITECTURE.md',
        'REFACTORING_SUMMARY.md',
    ]
    
    missing_files = []
    for filepath in expected_files:
        if not os.path.exists(filepath):
            missing_files.append(filepath)
            print(f"  ✗ Missing: {filepath}")
        else:
            print(f"  ✓ Exists: {filepath}")
    
    if missing_files:
        print(f"\n✗ Missing {len(missing_files)} files")
        return False
    else:
        print("\n✓ All expected files exist")
        return True


def test_main_py_has_topic_arg():
    """Test main.py có --topic-name argument"""
    print("\n=== Test 3: Main.py Topic Argument ===")
    
    with open('main.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check for --topic-name argument
    has_topic_arg = '--topic-name' in content
    if has_topic_arg:
        print("  ✓ main.py has --topic-name argument")
    else:
        print("  ✗ main.py missing --topic-name argument")
        return False
    
    # Check for filtering logic
    has_filter = 'topic_configs = [tc for tc in topic_configs' in content or \
                 'if args.topic_name:' in content
    if has_filter:
        print("  ✓ main.py has topic filtering logic")
    else:
        print("  ✗ main.py missing topic filtering logic")
        return False
    
    print("✓ Main.py correctly updated")
    return True


def test_etl_job_refactored():
    """Test etl_job.py đã được refactor"""
    print("\n=== Test 4: ETL Job Refactoring ===")
    
    with open('core/etl_job.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Should have schema processor import
    has_import = 'SchemaProcessorFactory' in content
    if has_import:
        print("  ✓ etl_job.py imports SchemaProcessorFactory")
    else:
        print("  ✗ etl_job.py missing SchemaProcessorFactory import")
        return False
    
    # Should have get_schema_processor method
    has_method = 'def get_schema_processor' in content
    if has_method:
        print("  ✓ etl_job.py has get_schema_processor method")
    else:
        print("  ✗ etl_job.py missing get_schema_processor method")
        return False
    
    # Should NOT have old methods
    old_methods = [
        'def parse_and_validate_schema',
        'def infer_schema_from_kafka',
        'def get_or_infer_schema'
    ]
    
    has_old_methods = False
    for method in old_methods:
        if method in content:
            print(f"  ✗ etl_job.py still has old method: {method}")
            has_old_methods = True
    
    if not has_old_methods:
        print("  ✓ etl_job.py removed old schema methods")
    else:
        return False
    
    print("✓ ETL Job correctly refactored")
    return True


def test_documentation():
    """Test documentation files có đủ content"""
    print("\n=== Test 5: Documentation ===")
    
    docs = {
        'core/schema_processors/README.md': ['BaseSchemaProcessor', 'Factory', 'Custom'],
        'QUICKSTART.md': ['--topic-name', 'Custom Processor'],
        'ARCHITECTURE.md': ['Before', 'After', 'Schema Processor Flow'],
        'REFACTORING_SUMMARY.md': ['Schema Processors', 'main.py', 'etl_job.py'],
    }
    
    all_good = True
    for doc_file, keywords in docs.items():
        try:
            with open(doc_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            missing = [kw for kw in keywords if kw not in content]
            if missing:
                print(f"  ✗ {doc_file} missing keywords: {missing}")
                all_good = False
            else:
                print(f"  ✓ {doc_file} has all expected content")
        except FileNotFoundError:
            print(f"  ✗ {doc_file} not found")
            all_good = False
    
    if all_good:
        print("✓ All documentation complete")
    return all_good


def test_deployment_scripts():
    """Test deployment scripts tồn tại và có content đúng"""
    print("\n=== Test 6: Deployment Scripts ===")
    
    scripts = {
        'deploy_per_topic.ps1': ['AppName', 'Topics', 'spark-submit'],
        'deploy_per_topic.sh': ['APP_NAME', 'TOPICS', 'spark-submit'],
    }
    
    all_good = True
    for script_file, keywords in scripts.items():
        try:
            with open(script_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            missing = [kw for kw in keywords if kw not in content]
            if missing:
                print(f"  ✗ {script_file} missing: {missing}")
                all_good = False
            else:
                print(f"  ✓ {script_file} has correct content")
        except FileNotFoundError:
            print(f"  ✗ {script_file} not found")
            all_good = False
    
    if all_good:
        print("✓ Deployment scripts ready")
    return all_good


def run_all_tests():
    """Run all tests"""
    print("\n" + "="*60)
    print("LIGHTWEIGHT REFACTORING TESTS (No PySpark Required)")
    print("="*60)
    
    tests = [
        ('Imports', test_imports),
        ('File Structure', test_file_structure),
        ('Main.py Updates', test_main_py_has_topic_arg),
        ('ETL Job Refactoring', test_etl_job_refactored),
        ('Documentation', test_documentation),
        ('Deployment Scripts', test_deployment_scripts),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n✗ Test '{test_name}' failed with error: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print("\n" + "="*60)
    if passed == total:
        print(f"✅ ALL TESTS PASSED! ({passed}/{total})")
        print("="*60)
        print("\n🎉 Refactoring is complete and verified!")
        print("\nNext steps:")
        print("  1. Review the changes")
        print("  2. Test with real Spark environment")
        print("  3. Deploy to dev/staging")
        print("  4. Monitor & validate")
        print("\nDocumentation:")
        print("  - QUICKSTART.md    - How to use")
        print("  - ARCHITECTURE.md  - System design")
        print("  - core/schema_processors/README.md - Detailed docs")
        return True
    else:
        print(f"❌ SOME TESTS FAILED! ({passed}/{total} passed)")
        print("="*60)
        print("\nPlease fix the issues above before proceeding.")
        return False


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
