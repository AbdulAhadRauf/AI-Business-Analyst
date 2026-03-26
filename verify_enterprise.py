"""Quick verification that all enterprise modules load correctly."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_yaml_config():
    import yaml
    with open("business_config.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    assert config["company"]["name"] == "Pizza Express"
    assert "adapters" in config
    assert "auth" in config
    assert "business_rules" in config
    print("  PASS: YAML config loads correctly")
    print(f"   Company: {config['company']['name']}")
    print(f"   Adapters: {list(config['adapters'].keys())}")
    print(f"   Auth strategy: {config['auth']['strategy']}")
    print(f"   Rule categories: {list(config['business_rules'].keys())}")

def test_base_adapter():
    from app.adapters.base_adapter import BaseDataAdapter, AdapterResult, AdapterSchema
    r = AdapterResult(items=[{"id": 1}], total=1)
    assert r.total == 1
    print("  PASS: base_adapter imports OK")

def test_json_adapter():
    from app.adapters.json_adapter import JSONAdapter
    adapter = JSONAdapter(
        source_name="test",
        source="data/customers.json",
        id_field="customer_id",
        search_fields=["name", "email"],
        display_name="Test",
    )
    schema = adapter.get_schema()
    tools = adapter.get_tool_definitions()
    print(f"  PASS: json_adapter OK - schema has {len(schema.fields)} fields, {len(tools)} tools")

def test_session():
    from app.sessions.session import Session
    from app.sessions.session_store import SessionStore
    store = SessionStore()
    s = store.create_session(channel="voice")
    assert s.authenticated == False
    assert store.get_active_count() == 1
    print(f"  PASS: sessions OK - created session {s.id[:8]}")

def test_rule_engine():
    import yaml
    from app.rules.rule_engine import RuleEngine
    from app.rules.rule_models import RuleEvalContext
    with open("business_config.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    engine = RuleEngine(config.get("business_rules", {}))
    
    ctx = RuleEvalContext(order={"status": "in_transit"})
    guard = engine.check_guard("update_order", ctx)
    assert guard is not None
    assert "on the way" in guard.message.lower() or "way" in guard.message.lower()
    
    ctx2 = RuleEvalContext(customer={"days_since_last_order": 20})
    greetings = engine.get_greeting_messages(ctx2)
    assert len(greetings) > 0
    
    print(f"  PASS: rule_engine OK - guard blocks in-transit, greeting rules work")

def test_adapter_registry():
    from app.adapters.adapter_registry import AdapterRegistry
    registry = AdapterRegistry()
    registry.load_adapters()
    names = registry.get_adapter_names()
    assert len(names) >= 3
    all_tools = registry.get_all_tool_definitions()
    print(f"  PASS: adapter_registry OK - {len(names)} adapters, {len(all_tools)} tools")

if __name__ == "__main__":
    tests = [
        test_yaml_config,
        test_base_adapter,
        test_json_adapter,
        test_session,
        test_rule_engine,
        test_adapter_registry,
    ]
    
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {test.__name__} - {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
