import time
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.hooks.http import HttpHook

 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'keywordmon_naver_datalab',
    default_args=default_args,
    schedule_interval='0 2 * * 4',
)

def get_rank_func(**kwargs):
    cidToCategory = {
        # "50000000": "패션의류",
        # "50000001": "패션잡화",
        # "50000002": "화장품/미용",
        # "50000003": "디지털/가전",
        # "50000004": "가구/인테리어",
        # "50000005": "출산/육아",
        # "50000006": "식품",
        # "50000007": "스포츠/레저",
        # "50000008": "생활/건강",
        # "50000009": "여가/생활편의",
        "50000010": "면세점",
        # "50005542": "도서",
    }
    result = []
    for cid, category in cidToCategory.items():
        response = requests.get(f'http://naver-datalab-functions.vacuum.10.0.0.241.sslip.io/?cid={cid}&maxPage=6')
        data = response.json()
        ranks = [r['ranks'] for r in data]
        ranks_flat = [item for sublist in ranks for item in sublist]
        keywords = [r['keyword'] for r in ranks_flat]
        result.append({
            'category': category,
            'keywords': keywords,
        })
        time.sleep(5)
    return result

get_rank = PythonOperator(
    task_id='get_rank',
    python_callable=get_rank_func,
    provide_context=True,
    dag=dag,
)
 
 
def post_rank_func(**kwargs):
    ti = kwargs['ti']
    get_rank_result = ti.xcom_pull(task_ids='get_rank')
    result = []
    for item in get_rank_result:
        data = {
            'requestName': item['category'],
            'keywords': item['keywords'],
            'type': 'narrow'
        }
        response = requests.post('http://10.0.0.241:30102/keywords/create/bulk', json=data)
        result.append({
            'requestName': item['category'],
            'status': response.status_code,
        })
        time.sleep(2)
    return result

post_rank = PythonOperator(
    task_id='post_rank',
    python_callable=post_rank_func,
    provide_context=True,
    dag=dag,
)

def send_to_slack_func(**kwargs):
    ti = kwargs['ti']
    post_rank_result = ti.xcom_pull(task_ids='post_rank')
    messages = [f"Request Name: {item['requestName']}, Status: {item['status']}" for item in post_rank_result]
    data = {
        'icon_emoji': ':sparkles:',
        'text': "keywordmon에 새 키워드셋을 요청했어요\n" + "\n".join(messages)
    }
    response = requests.post('https://hooks.slack.com/services/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', json=data)
    print(response)

send_to_slack = PythonOperator(
    task_id='send_to_slack',
    python_callable=send_to_slack_func,
    provide_context=True,
    dag=dag,
)

get_rank >> post_rank >> send_to_slack
