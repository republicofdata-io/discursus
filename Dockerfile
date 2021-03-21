FROM ubuntu:latest

# Set up environment
RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app /opt/dagster/app/miners /opt/dagster/app/dw /opt/dagster/app/orchestration

RUN apt-get update && apt-get install -y \
    software-properties-common
RUN add-apt-repository universe
RUN apt-get update && apt-get install -y \
    curl \
    git \
    python3.4 \
    python3-pip \
    zsh

RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.1.1/zsh-in-docker.sh)"

RUN pip3 install dagster dagit dagster-postgres dagster_shell dagster_snowflake dagster_dbt


# Copy resources to /opt/dagster/app
COPY miners/ /opt/dagster/app/miners/
COPY dw/ /opt/dagster/app/dw/
COPY orchestration/ /opt/dagster/app/orchestration/


# Run
WORKDIR /opt/dagster/app/orchestration
EXPOSE 3000
ENTRYPOINT ["dagit", "-h", "0.0.0.0", "-p", "3000"]