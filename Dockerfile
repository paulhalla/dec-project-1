FROM python:3.9

WORKDIR /src

COPY / .

RUN pip install -r requirements.txt

ENV PYTHONPATH=/src

CMD ["python", "src/fin_elt/pipelines/fin_pipeline.py"]