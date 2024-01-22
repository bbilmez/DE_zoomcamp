from prefect.deployments import Deployment
from prefect.infrastructure.container import DockerContainer
from etl_web_to_gcs import etl_parent_flow

docker_block = DockerContainer.load("zoom")

docker_dep = Deployment.build_from_flow(
    flow = etl_parent_flow,
    name = 'docker-flow',
    # infrastucture = docker_block
)

if __name__ == "__main__":
    docker_dep.apply()
    