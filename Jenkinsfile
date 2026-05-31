@Library('tools') _

// poorbricks CI/CD on Jenkins — Spark medallion ETL framework. Mirrors the
// CircleCI test+deploy workflows via the `tools` library. Tests run on every
// branch (and gate the deploy by sequential ordering); publish + build + the two
// k8s deploys run on main.
//
// All test stages use the Spark-capable databricks image with mongo:7 + postgres
// sidecars (poorbricks's docker_images) and install deps with privateRegistry:false
// (public PyPI only — poorbricks-framework lives here, no Artifact Registry).
pipeline {
  agent none
  options {
    disableConcurrentBuilds()
    retry(count: 2, conditions: [agent()])   // retry only on agent loss (spot eviction), not real failures
  }

  stages {
    stage('test') {
      parallel {
        stage('lint-and-type-check') {
          agent { kubernetes { yaml podTemplates.python(image: 'docker.io/danielspeixoto/databricks', cpu: '500m', memory: '1Gi', memoryLimit: '4Gi') } }
          steps {
            checkout scm
            installPythonDeps(privateRegistry: false)
            container('python') {
              sh 'poetry run ruff check .'
              sh 'poetry run ruff format --check .'
              sh 'poetry run mypy poorbricks/ utils/ validation/'
            }
          }
        }
        stage('tests') {
          agent { kubernetes { yaml podTemplates.python(image: 'docker.io/danielspeixoto/databricks', mongo: true, postgres: true, databaseImage: 'mongo:7', cpu: '1', memory: '2Gi', memoryLimit: '8Gi') } }
          steps {
            checkout scm
            installPythonDeps(privateRegistry: false)
            container('python') {
              waitForPortsReady(ports: '27017')
              script {
                try {
                  sh 'poetry run pytest poorbricks/ utils/ tables/ validation/ api/ -n 2 -m "not integration and not slow"'
                  sh 'poetry run poorbricks verify --mode arch'
                } finally { junit testResults: 'test-results/junit.xml', allowEmptyResults: true }
              }
            }
          }
        }
        stage('multi-repo-tests') {
          agent { kubernetes { yaml podTemplates.python(image: 'docker.io/danielspeixoto/databricks', mongo: true, postgres: true, databaseImage: 'mongo:7', cpu: '1', memory: '2Gi', memoryLimit: '8Gi') } }
          steps {
            checkout scm
            installPythonDeps(privateRegistry: false)
            container('python') {
              script {
                try {
                  sh 'poetry run pytest tests/test_multi_repo.py -o "addopts=" --junitxml=test-results/junit.xml --tb=short -v'
                } finally { junit testResults: 'test-results/junit.xml', allowEmptyResults: true }
              }
            }
          }
        }
        stage('build-and-smoke') {
          agent { kubernetes { yaml podTemplates.python(image: 'docker.io/danielspeixoto/databricks', mongo: true, postgres: true, databaseImage: 'mongo:7', cpu: '1', memory: '2Gi', memoryLimit: '12Gi') } }
          steps {
            checkout scm
            installPythonDeps(privateRegistry: false)
            container('python') {
              waitForPortsReady(ports: '27017')
              script {
                try {
                  sh 'poetry run pytest tests/test_wheel_install_boundary.py -m slow -n 0'
                } finally { junit testResults: 'test-results/junit.xml', allowEmptyResults: true }
              }
            }
          }
        }
        stage('integration-tests') {
          agent { kubernetes { yaml podTemplates.python(image: 'docker.io/danielspeixoto/databricks', mongo: true, postgres: true, databaseImage: 'mongo:7', cpu: '1', memory: '2Gi', memoryLimit: '12Gi') } }
          steps {
            checkout scm
            installPythonDeps(privateRegistry: false)
            container('python') {
              waitForPortsReady(ports: '27017 5432')
              script {
                try {
                  sh 'poetry run pytest tests/test_distributed_pipeline.py -m integration -n 0'
                } finally { junit testResults: 'test-results/junit.xml', allowEmptyResults: true }
              }
            }
          }
        }
        stage('workflow-compilation-tests') {
          agent { kubernetes { yaml podTemplates.python(image: 'docker.io/danielspeixoto/databricks', cpu: '500m', memory: '1Gi', memoryLimit: '4Gi') } }
          steps {
            checkout scm
            installPythonDeps(privateRegistry: false)
            container('python') {
              script {
                try {
                  sh 'poetry run pytest tests/test_infrastructure_e2e.py'
                } finally { junit testResults: 'test-results/junit.xml', allowEmptyResults: true }
              }
            }
          }
        }
      }
    }

    stage('publish + build') {
      when { branch 'main' }
      parallel {
        stage('publish-python-package') {
          when { expression { return false } }  // HELD: poorbricks publish paused (work in progress)
          agent { kubernetes { yaml podTemplates.gke() } }
          steps { checkout scm; publishPythonPackage() }
          post { failure { container('gke') { notifySlack(event: 'fail') } } }
        }
        stage('build-docker') {
          agent { kubernetes { yaml podTemplates.kaniko() } }
          steps { checkout scm; buildDocker(image: 'poorbricks/api', dockerfile: 'api/Dockerfile') }
        }
      }
    }

    stage('deploy') {
      when { branch 'main' }  // restored: poorbricks deploys run on main again
      parallel {
        stage('deploy-api') {
          agent { kubernetes { yaml podTemplates.gke() } }
          steps { checkout scm; deployK8s(appName: 'poorbricks-api', kubernetesDirectory: 'deploy/k8s/api', canary: false, generateK8sConfig: false) }
          post { failure { container('gke') { notifySlack(event: 'fail') } } }
        }
        stage('deploy-streamlit') {
          agent { kubernetes { yaml podTemplates.gke() } }
          steps { checkout scm; deployK8s(appName: 'poorbricks-streamlit', kubernetesDirectory: 'deploy/k8s/streamlit', canary: false, generateK8sConfig: false) }
          post { failure { container('gke') { notifySlack(event: 'fail') } } }
        }
      }
    }
  }
}
