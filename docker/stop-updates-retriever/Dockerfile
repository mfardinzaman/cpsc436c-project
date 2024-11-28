FROM public.ecr.aws/lambda/python:3.11

COPY stop-updates-retriever.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt .
COPY sf-class2-root.crt .

RUN pip install -r requirements.txt

CMD [ "stop-updates-retriever.lambda_handler" ]