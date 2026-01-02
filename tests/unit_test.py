def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"


def test_channel_handle(channel_handle):
    assert channel_handle == "MRCHEESE"


def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.host == "mock_host"
    assert conn.schema == "mock_db_name"
    assert conn.login == "mock_user"
    assert conn.password == "mock_password"
    assert conn.port == 1234


def test_dags_integrity(dagbag):
    #1.
    assert dagbag.import_errors == {}, f"DAG import errors found: {dagbag.import_errors}"
    print("========")
    print(dagbag.import_errors)

    #2.
    expected_dags = ['produdce_json', 'update_db', 'data_quality']
    loaded_dag_ids = list(dagbag.dags.keys())
    print("========")
    print(dagbag.dags.keys())

    for dag_id in expected_dags:
        assert dag_id in loaded_dag_ids, f"DAG '{dag_id}' is missing in the DagBag"

    #3.
    assert dagbag.size() == len(expected_dags), f"Expected {len(expected_dags)} DAGs, but found {dagbag.size()}"
    print("=======")
    print(dagbag.size())

    #4.

    expected_task_counts = {
        'produdce_json': 5,
        'update_db': 3,
        'data_quality': 2
    }

    print("=======")

    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)
        assert( expected_count == actual_count),f"DAG '{dag_id}' should have {expected_count} tasks, but has {actual_count}"

        print(f"DAG '{dag_id}' has {actual_count} tasks.")