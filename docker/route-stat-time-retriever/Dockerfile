FROM public.ecr.aws/lambda/python:3.11

COPY route-stat-time-retriever.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt .
COPY sf-class2-root.crt .

RUN pip install -r requirements.txt

CMD [ "route-stat-time-retriever.lambda_handler" ]