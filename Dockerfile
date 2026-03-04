FROM public.ecr.aws/lambda/python:3.13

RUN python -m pip install --no-cache-dir \
    boto3 \
    requests \
    beautifulsoup4 \
    lxml \
    pydantic \
    litellm \
    yfinance \
    pyyaml \
    click \
    python-dotenv \
    anthropic

COPY src ${LAMBDA_TASK_ROOT}/src

# Default command; deploy script overrides per function with ImageConfig.Command.
CMD ["src.modules.events.eight_k_scanner.poller_handler.lambda_handler"]
