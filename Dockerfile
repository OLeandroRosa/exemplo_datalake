FROM apache/airflow:2.10.5-python3.10
USER root

# -----------------------------
# Pacotes básicos e JDK 11
# -----------------------------
RUN apt-get update && apt-get install -y wget gnupg software-properties-common \
    && echo "deb http://deb.debian.org/debian bullseye main" > /etc/apt/sources.list.d/bullseye.list \
    && apt-get update \
    && apt-get install -y openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# -----------------------------
# Google Chrome Stable
# -----------------------------
RUN wget -qO- https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/google-chrome
ENV PATH="$PATH:/usr/bin"

# Verificações opcionais
RUN java -version
RUN echo $JAVA_HOME

# -----------------------------
# Alterna para o usuário airflow
# -----------------------------
USER airflow

# Corrige PATH do airflow instalado no container base e pacotes pip
ENV PATH="/opt/airflow/.local/bin:/usr/local/bin:$PATH"


# Para evitar conflitos de versão das bibliotecas, os pacotes principais tiveram a versão fixada
RUN pip install --upgrade pip
RUN pip install apache-airflow-providers-amazon==9.3.0
RUN pip install pyspark==3.2.0
RUN pip install deltalake==0.23.2
RUN pip install delta-spark==1.2.1
RUN pip install pandas
RUN pip install scipy
RUN pip install pyarrow
RUN pip install awswrangler
RUN pip install s3fs
RUN pip install boto3==1.34.90
RUN pip install joblib==1.5.1
RUN pip install requests==2.32.3
run pip install imblearn
run pip install xgboost==2.1.4
RUN pip install scikit-learn==1.6.1