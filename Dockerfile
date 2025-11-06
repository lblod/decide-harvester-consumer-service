FROM semtech/mu-javascript-template:feature-query-meta
LABEL maintainer="Nordine Bittich <contact@bittich.be>"
RUN apt update && apt upgrade -y 
ENV SUDO_QUERY_RETRY="true"
ENV SUDO_QUERY_RETRY_FOR_HTTP_STATUS_CODES="404,500,503"
RUN curl -sSfL https://raw.githubusercontent.com/anchore/grype/main/install.sh | sh -s -- -b /bin
# see https://github.com/mu-semtech/mu-javascript-template for more info
