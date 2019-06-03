# Deid DLP and Evaluation Frontend

This package contains a Graphical User Interface (GUI) that runs DLP and
EVAL command line tools in a visual web interface. Currently, the frontend
and server run separately and locally on two different ports. The server uses
the [bazel build system](http://bazel.build/versions/master/docs/install.html).
The frontend on the other hand is built with angular and uses the Angular Cli
to be run.

## Setup

If you are planning to use this tool on real PHI, ensure your project and
permissions follow HIPAA guidelines.

### Authentication

In order to run the pipeline properly, follow the setup authentication
instructions in the [dlp pipeline
subdirectory](../dlp#authentication).

### Setting up The Frontend

In order for the frontend to be ready, first make sure that you have the
[Angular Cli](https://github.com/angular/angular-cli/wiki) installed. If you
need to install it, run these commands:

```shell
npm install -g @angular/cli
```

After that, from the main directory, install the required dependencies for the
frontend:

```shell
cd deid_app/frontend
npm install
```

### Setting up The Database

This app leverages [CloudSQL](https://cloud.google.com/sql/docs/) to store and
organize its information. In order to create an instance, follow [this
guide](https://cloud.google.com/sql/docs/mysql/create-instance) that will show
you how to create a MySQL CloudSQL instance.

After creating an instance, you will need proxy to forward the database queries
to CloudSQL. In order to do this, follow steps #1 - #4 [here](https://cloud.google.com/sql/docs/mysql/connect-admin-proxy).
(Use the same service account you have from the DLP pipeline instead of creating
a new one). Use the TCP sockets in step 4.

Keep the terminal tab that has the CloudSQL proxy running open.

Create a new database from the CloudSQL instance you created. Inside the
instance's main page, navigate to the DATABASES tab and create a new database.
Use that one when specifying the environment variables.

While creating the database, you will enter the password for the root user. You
can either use this root user when entering the database credentials, or you can
go to the USERS tab and create a new user to use with this app.

### Setting up The Server

The server leverages Flask and SqlAlchemy, which need some dependencies to be
installed first. The app requires Python version 2.7.x. Check that you have this
version by running:

```shell
python --version
```

The code for the pipeline is available for download from GitHub:

```shell
git clone https://github.com/GoogleCloudPlatform/healthcare-deid.git && \
cd healthcare-deid
```

Running the server requires both the dependencies necessary to run flask and the
dependencies necessary to run the pipeline. The flask server requires
having Flask, jsonschema, sqlalchemy, pymysql, flask_sqlalchemy. The dlp
requires having the Google Python API client, Google Cloud
Storage client, and Python Apache Beam client installed. Note that as of
2017-10-27, there is an incompatibility with the latest version of the
`six` library, which requires a downgrade to 1.10.0 to fix. These commands must
be run from the main directory.

```shell
virtualenv env
source env/bin/activate
pip install --upgrade apache_beam[gcp] google-api-python-client google-cloud-bigquery google-auth-httplib2 google-cloud-storage six==1.10.0 flask jsonschema sqlalchemy pymysql==0.8.1 flask_sqlalchemy
```

After the database proxy has been successfully created, add the user and
password information of the CloudSQL instance into the environment variables.
The information can be found in the CloudSQL instance's [main page](https://console.cloud.google.com/sql/instances),
specifically, assuming your instance name is "deidapp":

* CLOUDSQL_USER: https://console.cloud.google.com/sql/instances/deidapp/users
* CLOUDSQL_PASSWORD: Not shown in the UI, but you can reset it on the "users"
  page.
* CLOUDSQL_DATABASE: https://console.cloud.google.com/sql/instances/deidapp/databases
* CLOUDSQL_CONNECTION_NAME: "instance connection name" on the instance page:
  https://console.cloud.google.com/sql/instances/deidapp

```shell
export CLOUDSQL_USER="<user>"
export CLOUDSQL_PASSWORD="<password>"
export CLOUDSQL_DATABASE="<database>"
export CLOUDSQL_CONNECTION_NAME="<connection_name>"
```

To create the necessary tables inside CloudSQL, run this command:

```shell
bazel run deid_app/backend:model
```

### Running The Server

in order to run the server, execute this command:

```shell
bazel run deid_app/backend:server
```

### Running The Frontend

With the backend running, you can now run and access the frontend through a web
browser. From the main directory, run these commands in a new terminal shell:

```shell
cd deid_app/frontend
ng serve
```
