import requests

def notifyRecommendService(host="127.0.0.1", port=8081):
    resp = requests.post('http://%s:%s/actuator/refresh' % (host, port))
    #data = resp.json()
    #print(data)

if __name__ == "__main__":
    print("test")
    notifyRecommendService()
