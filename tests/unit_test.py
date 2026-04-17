def test_api_key(api_key):
    assert api_key == "Mock-KEY1234"

def test_channel_handle(channel_handle):
    assert channel_handle == "Mock-CHANNEL1234"

def test_mock_postgres_conn_vars(mock_postgres_conn_vars):
    assert mock_postgres_conn_vars.login == "mock_user"
    assert mock_postgres_conn_vars.password == "mock_password"
    assert mock_postgres_conn_vars.host == "mock_host"
    assert mock_postgres_conn_vars.port == 5432
    assert mock_postgres_conn_vars.schema == "mock_db"

def test_dags_integrity(dagbag):

    #1. Check if all DAGs are loaded without import errors
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"
    print("=======================")
    print(dagbag.import_errors)

    #2. Check if expected DAGs are present in the DagBag
    expected_dag_ids = {'produce_json', 'update_db', 'data_quality'}
    loaded_dag_ids = set(dagbag.dags.keys())
    print("=======================")
    print(dagbag.dags.keys())
    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG '{dag_id}' is not loaded in the DagBag"

    #3. Check if the total number of DAGs loaded matches the expected count
    assert dagbag.size() == 3
    print("=======================")
    print(f"Total DAGs loaded: {dagbag.size()}")

    #4. Check if each DAG has the expected number of tasks
    expected_task_counts = {
        'produce_json': 5,
        'update_db': 3,
        'data_quality': 2
    }
    print("=======================")
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts.get(dag_id)
        actual_count = len(dag.tasks)
        assert (
            expected_count == actual_count
        ), f"DAG '{dag_id}' has {actual_count} tasks, expected {expected_count}"
        print(dag_id, len(dag.tasks))