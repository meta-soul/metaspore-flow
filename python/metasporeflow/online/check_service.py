import requests


def checkRecommendService(host="127.0.0.1", port=9081):
    resp = requests.get('http://%s:%s/actuator/health/dataSource' % (host, port))
    data = resp.json()
    return data.get('status', "OUT_OF_SERVICE") == "UP"
