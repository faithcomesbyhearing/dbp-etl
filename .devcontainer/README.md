# DBP ETL Development Container

This directory contains the VS Code Dev Container configuration for the DBP ETL project.

## Overview

The dev container provides a complete, consistent development environment using Docker with:
- **Python 3.12.12** on Alpine Linux 3.21
- **VS Code extensions** pre-configured for Python development
- **AWS tools** and credentials integration
- **Live code reloading** for rapid development
- **External database connection** support (MySQL 8.0+)

## Quick Start

### Prerequisites

- **VS Code** with [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
- **Docker** 20.10+ and **Docker Compose** 2.0+
- At least 8GB RAM allocated to Docker
- At least 20GB free disk space

### Opening in Dev Container

1. **Clone the repository** (if you haven't):
   ```bash
   git clone <repository-url>
   cd dbp-etl
   ```

2. **Copy environment configuration**:
   ```bash
   cp .devcontainer/.env.example .devcontainer/.env
   # Edit .env with your actual values
   nano .devcontainer/.env
   ```

3. **Open in VS Code**:
   ```bash
   code .
   ```

4. **Reopen in Container**:
   - Press `F1` or `Ctrl+Shift+P`
   - Type: "Dev Containers: Reopen in Container"
   - Wait for container to build (first time takes 5-10 minutes)

5. **Start developing!**
   - Terminal opens in the container automatically
   - Code in `./load` directory is live-mounted
   - Changes are immediately reflected

## File Structure

```
.devcontainer/
├── devcontainer.json           # VS Code dev container config
├── Dockerfile                  # Dev container-specific Dockerfile
├── docker-compose.yml          # Docker service definition (DBP ETL)
├── entrypoint.sh               # Container entrypoint script
├── post-create.sh              # Post-creation setup script
├── .env                        # Environment variables (git-ignored)
├── .env.example               # Environment template
└── README.md                  # This file
```

## Configuration Files

### devcontainer.json

Main configuration for VS Code. Defines:
- Docker Compose setup
- VS Code extensions to install
- Python interpreter path
- Editor settings
- Remote user configuration
- Post-creation script reference

**Key settings:**
- Python interpreter: `/app/venv/bin/python`
- Workspace folder: `/app`
- User: `vscode` (non-root with sudo access)
- Extensions: Python, Pylance, Black, Docker, etc.

### Dockerfile

Dev container-specific Dockerfile based on the root Dockerfile with additions for development:
- **Git, sudo, shadow** for version control and user management
- **vscode user** (UID/GID configurable) for proper file permissions
- **Enhanced permissions** for seamless VS Code integration

**Key differences from root Dockerfile:**
- Adds development tools (git, sudo, shadow)
- Creates vscode user with sudo access
- Sets proper ownership for /app directory
- Maintains same Python 3.12 + Alpine 3.21 base
- Uses same requirements.txt with pinned versions

### docker-compose.yml

Defines the DBP ETL service for development with:
- Builds using `.devcontainer/Dockerfile`
- Volume mounts for live code editing
- Environment variables from `.env`
- AWS credentials and config mounting
- Resource limits for development
- Network configuration for external database access
- References `entrypoint.sh` for container startup

### entrypoint.sh

Container entrypoint script that runs when the container starts:
- Creates vscode home directory
- Sets up shell profile for automatic venv activation
- Displays ready message
- Keeps container running for interactive use (`tail -f /dev/null`)

**Purpose:**
- Ensures container stays alive for VS Code to attach
- Configures interactive shells to use Python virtual environment
- Separates startup logic from docker-compose configuration

### post-create.sh

Post-creation setup script that runs automatically after the container is created:
- Configures git safe directory
- Creates vscode home directory
- Sets up shell profile for automatic venv activation
- Displays welcome message with environment info
- Verifies Python packages are installed correctly

**What it does:**
- Auto-activates Python virtual environment in shell
- Shows Python version and installed packages
- Provides visual confirmation that setup succeeded

**Difference between entrypoint.sh and post-create.sh:**
- `entrypoint.sh` - Runs **every time** the container starts (lightweight, runtime setup)
- `post-create.sh` - Runs **once** after VS Code creates the container (heavier setup, verification)

### .env

Environment variables for the dev container. Copy from `.env.example` and customize.

**Required variables:**
- Database credentials
- S3 bucket names
- S3 Zipper credentials
- AWS region

**Optional variables:**
- AWS credentials (or use ~/.aws mount)
- ECS transcoder settings
- Monday.com API keys
- Audio transcoder URL

## Features

### Python Development

- **Linting**: Flake8 enabled
- **Formatting**: Black formatter on save
- **Testing**: unittest test discovery
- **IntelliSense**: Pylance for code completion
- **Debugging**: Python debugger configured

### VS Code Extensions

Pre-installed extensions:
- Python (ms-python.python)
- Pylance (ms-python.vscode-pylance)
- Black Formatter (ms-python.black-formatter)
- Docker (ms-azuretools.vscode-docker)
- YAML (redhat.vscode-yaml)
- Makefile Tools (ms-vscode.makefile-tools)
- Spell Checker (streetsidesoftware.code-spell-checker)

### Database Access

The dev container connects to an external MySQL 8.0+ database (not included in the container).

**Configure database connection in `.env`:**
```bash
DATABASE_HOST=your-db-host
DATABASE_PORT=3306
DATABASE_DB_NAME=dbp
DATABASE_USER=your-username
DATABASE_PASSWD=your-password
```

**Connect from container:**
```bash
mysql -h ${DATABASE_HOST} -u ${DATABASE_USER} -p${DATABASE_PASSWD} ${DATABASE_DB_NAME}
```

**Database requirements:**
- MySQL 8.0+ (or compatible)
- User must use `mysql_native_password` authentication (see Troubleshooting)
- Database should exist before running ETL

### AWS Integration

AWS credentials from `~/.aws/` are automatically mounted read-only.

**Test AWS access:**
```bash
aws s3 ls
```

**Use specific profile:**
```bash
export AWS_PROFILE=ecs-task-role
aws s3 ls s3://dbp-staging
```

### Live Code Reloading

The `./load` directory is mounted into the container. Changes to Python files are immediately available:

1. Edit a file in VS Code: `load/YourFile.py`
2. Run immediately: `python3 load/YourFile.py`
3. No rebuild needed!

## Common Tasks

### Run Unit Tests

```bash
# All tests
python3 load/TestFilenameParser.py
python3 load/TestMetadataReader.py
python3 load/TestLanguageReaderStage.py
python3 load/TestMondayHTTP.py
python3 load/TestUpdateDBPBibleFileTags.py
python3 load/TestUpdateDBPFilesetTables.py

# Or use VS Code Test Explorer
# (appears in left sidebar after container loads)
```

### Run ETL Pipeline

```bash
# Process from S3
python3 load/DBPLoadController.py data s3://dbp-staging FILESET_ID

# Metadata only
python3 load/DBPLoadController.py data
```

### Access External Database

```bash
# Using environment variables from .env
mysql -h ${DATABASE_HOST} -u ${DATABASE_USER} -p${DATABASE_PASSWD} ${DATABASE_DB_NAME}

# Or specify directly
mysql -h your-db-host -u your-user -pyour-password dbp
```

### Rebuild Container

If you change `Dockerfile` or `requirements.txt`:

1. **From VS Code:**
   - `F1` → "Dev Containers: Rebuild Container"

2. **From command line:**
   ```bash
   docker-compose -f .devcontainer/docker-compose.yml build --no-cache
   ```

### View Logs

```bash
# From another terminal (outside container)
docker-compose -f .devcontainer/docker-compose.yml logs -f dbp-etl
```

### Stop Container

**From VS Code:**
- `F1` → "Dev Containers: Reopen Folder Locally"

**From command line:**
```bash
docker-compose -f .devcontainer/docker-compose.yml down
```

## Troubleshooting

### Container won't build

**Check Docker resources:**
```bash
docker system df
docker system prune  # Clean up if needed
```

**Rebuild from scratch:**
- `F1` → "Dev Containers: Rebuild Container Without Cache"

### MySQL connection fails

**Check database host is reachable:**
```bash
ping ${DATABASE_HOST}
telnet ${DATABASE_HOST} 3306
```

**Verify credentials in .env file:**
```bash
cat .devcontainer/.env | grep DATABASE
```

**Check MySQL authentication method:**
The dev container requires MySQL users to use `mysql_native_password` authentication. If you get a `caching_sha2_password` error, run this on your MySQL server:

```sql
ALTER USER 'your_username'@'%' IDENTIFIED WITH mysql_native_password BY 'your_password';
FLUSH PRIVILEGES;
```

**Common issues:**
- Firewall blocking port 3306
- Database host not accessible from Docker
- Wrong credentials in .env file
- User not using mysql_native_password authentication

### biblebrain.services.base_url not accessible

The `biblebrain.services.base_url` (configured in `/home/vscode/dbp-etl.cfg`) is an API service that runs on the host machine on port 3009.

**To connect the dev container to the host machine network:**

```bash
# Connect the dev container to the host's docker network
docker network connect biblebrain-services_devcontainer_app-net dbp-etl-dev
```

**Verify connectivity:**
```bash
# From inside the dev container
curl http://biblebrain-services_devcontainer-app-1:3009/api/status
# or whatever health endpoint the biblebrain service provides
```

### AWS credentials not working

**Verify mount:**
```bash
ls -la ~/.aws
# Should show credentials and config files
```

**Test from container:**
```bash
aws configure list
aws s3 ls
```

### Python packages missing

**Verify virtual environment:**
```bash
which python3
# Should be: /app/venv/bin/python3

pip list
# Should show boto3, pymysql, etc.
```

**Reinstall if needed:**
```bash
pip install --no-cache-dir -r requirements.txt
```

### Changes not reflected

**For code changes:**
- Python files in `load/` should reflect immediately
- No restart needed

**For Dockerfile/requirements changes:**
- Need to rebuild container (see above)

### Out of memory

**Increase Docker memory:**
- Docker Desktop → Settings → Resources → Memory
- Allocate at least 8GB

## Advanced Usage

### Custom Extensions

Add to `devcontainer.json`:
```json
"extensions": [
  "your-extension-id"
]
```

### Custom Settings

Create `.devcontainer/docker-compose.override.yml`:
```yaml
version: '3.8'
services:
  dbp-etl:
    environment:
      DEBUG: "true"
```

### Multiple Terminals

VS Code automatically opens terminals in the container:
- `Ctrl+` \` ` - Toggle terminal
- `Ctrl+Shift+` \` ` - New terminal

All terminals run inside the container automatically.

### Debugging Python

1. Set breakpoints in VS Code (click left of line numbers)
2. Press `F5` to start debugging
3. Select "Python: Current File"
4. Debugger stops at breakpoints

Or use `pdb`:
```python
import pdb; pdb.set_trace()
```

## Differences from Root docker-compose.yml

This .devcontainer setup is **specifically for VS Code development** and uses its own Dockerfile.

**Key Differences:**
- `.devcontainer/Dockerfile` - Dev-focused with git, sudo, vscode user
- `Dockerfile` (root) - Production-focused, minimal dependencies
- Both use same `requirements.txt` for consistency

For **standalone docker-compose** (without VS Code), see `../docker-compose.yml` in the root or refer to `DOCKER_COMPOSE.md`.

## Resources

- [VS Code Dev Containers Documentation](https://code.visualstudio.com/docs/devcontainers/containers)
- [Dev Container Specification](https://containers.dev/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## Support

For issues with:
- **Dev Container**: Check VS Code Output panel → "Dev Containers"
- **Docker**: Run `docker-compose -f .devcontainer/docker-compose.yml logs`
- **Python**: Check terminal output and `pip list`

