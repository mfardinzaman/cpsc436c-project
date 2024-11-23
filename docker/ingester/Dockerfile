FROM public.ecr.aws/lambda/python:3.11

COPY ingester.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt .
COPY sf-class2-root.crt .

RUN pip install -r requirements.txt

CMD [ "ingester.lambda_handler" ]