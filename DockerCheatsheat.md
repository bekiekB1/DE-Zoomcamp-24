```bash
# Remove a specific Docker container by its container ID
docker rm <container_id>

# Create a custom Docker network with the specified name
docker network create <network_name>

# Build a Docker image from the Dockerfile in the current directory, tagging it with <image_name>:<tag>
docker build -t <image_name>:<tag> .

# Run a Docker container in interactive mode, allowing you to interact with the container's shell
docker run -it <image_name>:<tag>

# Remove all stopped containers to free up space and keep your environment clean
docker rm $(docker ps -a -q)

# Remove all Docker images from your local repository to free up space
docker rmi $(docker images -q)

# Run a Docker container in detached mode (in the background), allowing it to run independently
docker run -d <image_name>:<tag>

# List all running containers, showing their IDs and status
docker ps

# List all containers, both running and stopped, providing a complete overview of your container status
docker ps -a

# List all Docker images available in your local repository, showing their names and tags
docker images

# Start Docker Compose services defined in docker-compose.yml in the foreground, attaching to logs
docker compose up

# Start Docker Compose services in detached mode (in the background), allowing you to use the terminal for other commands
docker compose up -d

# Stop and remove Docker Compose services, along with their networks, defined in the current Compose file
docker compose down

# Start Docker Compose services defined in the main Compose file, using the project name "main" for resource isolation
docker-compose -p main up -d

# Stop and remove services defined in the specified docker-compose file (with a specific tag), along with their networks
docker-compose -f docker-compose.<tag>.yml down

# Stop and remove services defined in the specified docker-compose file (with a specific tag) using a custom project name
docker-compose -f docker-compose.<tag>.yml -p <project_name> down

# Stop and remove services defined in the main Compose file, using the project name "main" for resource isolation
docker-compose -p main down

# Run a one-off command in a service defined in the specified docker-compose file (ingest.yml), removing the container after it exits
docker-compose -f docker-compose.ingest.yml run --rm <service_name>

# Rebuilds the Docker image defined in docker-compose.yml without using the cache
# Eg python script content changes
docker-compose -f docker-compose.yml build --no-cache

# Remove all dangling images that are not tagged or associated with any containers
docker image prune
```