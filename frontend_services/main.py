from flask import Flask, render_template, flash, request
from google.cloud import datastore
import requests

# App config.
app = Flask(__name__)
global jobtype;

def create_client(project_id):
    return datastore.Client(project_id)

def projection_query(client, jobblocation, jobbtype):
    query = client.query(kind='finalProcessedTweets')
    query.add_filter('Location', '=', jobblocation)
    query.add_filter('Job_List', '=', jobbtype)
    query.order = ['-Company_Name']
    return list(query.fetch())

def projection_query1(client, jobblocation):
    query = client.query(kind='finalProcessedTweets')
    query.add_filter('Location', '=', jobblocation)
    query.order = ['-Company_Name']
    return list(query.fetch())

def projection_query2(client, jobbtype):
    query = client.query(kind='finalProcessedTweets')
    query.add_filter('Job_List', '=', jobbtype)
    query.order = ['-Company_Name']
    return list(query.fetch())

def projection_query3(client):
    query = client.query(kind='finalProcessedTweets')
    query.order = ['-Company_Name']
    return list(query.fetch())


PROJECT_ID = 'cloud-202303'
gCloudClient = create_client(PROJECT_ID)


@app.errorhandler(500)
def server_error(e):
    logging.exception('some eror')
    return """
    And internal error <pre>{}</pre>
    """.format(e), 500

# to test from the bash script
@app.route("/test", methods=['GET'])
def test():
    cities = []
    error = 'Results';
    jobtype = request.args.get('jobtype').replace('"','')
    joblocation = request.args.get('location').replace('"','')
    print jobtype
    print joblocation

    if jobtype == 'Anything':
        if joblocation == '' or joblocation == 'Bay Area':
            df = projection_query3(gCloudClient)
        else:
            df = projection_query1(gCloudClient, joblocation)

    else:
        if joblocation == '' or joblocation == 'Bay Area':
            df = projection_query2(gCloudClient, jobtype)
        else:
            df = projection_query(gCloudClient, joblocation, jobtype)


    list1=[]
    for row in df:
        s = ""
        if row['Job_Url'] is not None and len(row['Job_Url']) > 0:
            s = row['Job_Url'][0].split(', ')
            if len(s) > 0:
                s = s[-1]
        list1.append([s,row['Tweet'],row['Location'],row['Company_Name'],row['Job_List']])

    loc_set = set()
    for i in range(len(list1)):
        loc_set.add(list1[i][2].encode('ascii','ignore'))
    cities= list(loc_set)

    if len(list1) == 0:
        error = 'No Results Found';
    return render_template("index.html", result = list1, cities = cities, error= error)

# to load initially
@app.route("/", methods=['GET', 'POST'])
def index():
    return render_template('index.html')

@app.route("/get_information", methods=['POST', 'GET'])
def get_information():
    cities = []
    list1=[]
    cityMap = {
        'santaclara': 'Santa Clara',
        'sanjose': 'San Jose',
        'sunnyvale': 'Sunnyvale',
        "sanfrancisco":'San Francisco',
        'paloalto':'Palo Alto',
        'redwoodcity':'Redwood City',
        'southsanfrancisco':'South San Francisco'
    }
    error = 'Results';
    if request.method == 'POST':
        jobtype = request.form['jobtype']
        joblocation=request.form['joblocation']
        if jobtype == 'Anything':
            if joblocation == '' or joblocation == 'Bay Area':
                df = projection_query3(gCloudClient)
            else:
                df = projection_query1(gCloudClient, joblocation)

        else:
            if joblocation == '' or joblocation == 'Bay Area':
                df = projection_query2(gCloudClient, jobtype)
            else:
                df = projection_query(gCloudClient, joblocation, jobtype)


        for row in df:
            if row['Job_Url'] is not None and len(row['Job_Url']) > 0 and row['Job_Url'][0].strip() != "":
                s = row['Job_Url'][0].split(', ')
                if len(s) > 0:
                    s = s[-1]
                    list1.append([s,row['Tweet'],row['Location'],row['Company_Name'],row['Job_List']])

        loc_set = set()
        countArray = {}
        for i in range(len(list1)):
            a = ''.join(list1[i][2].encode('ascii','ignore').lower().split(' '))
            if(joblocation == 'Bay Area'):
                if cityMap.get(a,None) is not None:
                    a = cityMap.get(a, a)
                    countArray[a] = countArray.get(a, 0) + 1
            else:
                a = cityMap.get(a, a)
                countArray[a] = countArray.get(a, 0) + 1

        cities = ','.join(map(lambda x: x[0], countArray.items()))
        jobCount = ','.join(map(lambda x: str(x[1]), countArray.items()))
        print cities, jobCount
        total_len = len(list1);
        if len(list1) == 0:
            error = 'No Results Found';
        return render_template("index.html", result = list1, cities = cities, error= error, jobCount = jobCount)
    else:
        return render_template("index.html")


if __name__ == '__main__':
     app.run(host='0.0.0.0', port=8080, debug=True)
