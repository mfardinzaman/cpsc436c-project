FROM public.ecr.aws/lambda/python:3.11

COPY route-stats-retriever.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt .
COPY sf-class2-root.crt .

RUN pip install -r requirements.txt

CMD [ "route-stats-retriever.lambda_handler" ]