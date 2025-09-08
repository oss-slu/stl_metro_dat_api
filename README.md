# stl_metro_dat_api
**Overview**

The STL Data API project is a centralized, user-friendly platform designed to serve as a proxy for accessing and interacting with public data from various regional and municipal sources, with a focus on the St. Louis region. The project addresses challenges such as inconsistent data formats, lack of standardization, and repetitive efforts in compiling datasets by providing a RESTful API and a foundation for a future web portal. It is built using a CQRS (Command Query Responsibility Segregation) architecture with microservices, leveraging modern technologies for scalability and maintainability.

**Project Flow**

1. Data Ingestion: Fetch raw data (Excel, PDF, JSON, or web content) from public St. Louis data sources.
2. Raw Data Processing: Clean and transform raw data in memory, then send to Kafka for queuing.
3. Data Storage: Consume processed data from Kafka, store in PostgreSQL (snapshots, historic puts, aggregations), and delete raw data from memory.
4. Event Processing: Optimize short-term reads via event processors in the query-side microservice.
5. API Access: Expose RESTful endpoints (via Flask) for querying data, with Open API documentation.
6. Future Features: Add user subscriptions, web portal, and advanced optimizations.

**Tech Stack**

- Python 3.10+: Core language for data processing and API development.
- Flask Restful: Framework for building RESTful APIs in CQRS microservices.
- Kafka: Message broker for scalable, write-optimized data queuing (containerized).
- PostgreSQL: Database for storing processed data (containerized).
- Docker: Containerization for Kafka, PostgreSQL, and microservices.
- Open API (Swagger): API documentation for endpoints.
- SQLAlchemy: ORM for PostgreSQL interactions.

**Getting Started**

**Prerequisites**

- Python 3.10+: Install via python.org or pyenv.
- Docker Desktop: Install from docker.com (includes Docker Compose).
- psql Client: For PostgreSQL interaction (e.g., brew install postgresql on Mac).
- Git: For cloning the repository.
- VS Code: Recommended IDE with extensions (Python, Docker).