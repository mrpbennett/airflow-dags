1. **Your DAG definitions** (the “what runs when” files)
2. **Any shared libraries** (utilities, custom hooks/operators, etc.)
3. **Tests** and CI configuration
4. **Deployment artifacts** (Dockerfile, Helm charts, requirements.txt, etc.)

–––

## 1. Top-level layout

A common pattern is:

```
your-airflow-repo/
├── dags/                   ← DAG definitions live here
│   ├── __init__.py         ← so sub-packages can import shared code
│   ├── project_alpha/      ← split by business unit or feature
│   │   ├── __init__.py
│   │   └── alpha_dag.py
│   └── project_beta/
│       ├── __init__.py
│       └── beta_dag.py
│
├── src/                    ← your “library” code
│   └── airflow_utils/
│       ├── __init__.py
│       ├── hooks.py
│       └── operators.py
│
├── plugins/                ← if you need to ship custom plugins
│   └── sensors.py
│
├── tests/                  ← pytest or unittest suites
│   └── test_dags.py
│
├── requirements.txt        ← pin your Airflow & library deps
├── setup.py / pyproject.toml
├── Dockerfile              ← build an image that installs your code
└── .github/
    └── workflows/          ← CI for linting, “airflow dags list” checks
```

–––

## 2. Recursive discovery of DAGs

- **By default**, Airflow’s scheduler will _walk_ the entire `dags_folder` tree and load every `.py` file it finds.
- That means you can freely nest sub-directories (e.g. `dags/project_alpha/…`) and it will still see `alpha_dag.py`.
- If you ever need to limit recursion—for example, to ignore large support directories—you can adjust in `airflow.cfg`:

  ```ini
  [core]
  dags_folder = /opt/airflow/dags
  # dags_folder can also be a comma-separated list of paths:
  # dags_folder = /opt/airflow/dags,/opt/airflow/more_dags
  ```

–––

## 3. Keeping your DAGs thin

A recommended best practice is to **limit what lives in `dags/`** to just:

- DAG definitions (`with DAG(…) as dag: …`)
- Small wiring code—`from airflow_utils.hooks import MyHook` etc.

All heavy business logic, reusable functions, or long SQL‐templating should live under `src/airflow_utils` (or whatever you call it), and be installed into your image via `pip install -e .`. This:

- Speeds up DAG parsing
- Makes your DAG files easy to read
- Allows you to unit-test your business code in isolation

–––

## 4. Versioning & deployment

- **Single-repo**: keeps your CI/CD pipeline simple—one build that runs `pytest`, lints your DAGs, builds a Docker image, then pushes.
- **Semantic file names**: if you want DAGs to appear in a certain order in the UI, prefix them with dates or numbers (`2025_06_01__daily_cleanup.py`).
- **CI checks**: include a job that runs `airflow dags list` against your local files to catch syntax errors.
