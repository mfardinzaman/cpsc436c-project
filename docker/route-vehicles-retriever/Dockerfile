FROM public.ecr.aws/lambda/python:3.11

COPY route-vehicles-retriever.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt .
COPY sf-class2-root.crt .

RUN pip install -r requirements.txt

CMD [ "route-vehicles-retriever.lambda_handler" ]