import os
from flask import Flask, render_template, request, redirect, url_for, session
from google.cloud import bigquery, storage


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "creds.json"

app = Flask(__name__)
app.secret_key = "your_secret_key"
client = bigquery.Client()


schemas = {
    'transactions': [
        bigquery.SchemaField("HSHD_NUM", "INTEGER"),
        bigquery.SchemaField("BASKET_NUM", "INTEGER"),
        bigquery.SchemaField("PURCHASE_", "STRING"),
        bigquery.SchemaField("PRODUCT_NUM", "INTEGER"),
        bigquery.SchemaField("SPEND", "FLOAT"),
        bigquery.SchemaField("UNITS", "INTEGER"),
        bigquery.SchemaField("STORE_R", "STRING"),
        bigquery.SchemaField("WEEK_NUM", "INTEGER"),
        bigquery.SchemaField("YEAR", "INTEGER"),
    ],
    'households': [
        bigquery.SchemaField("HSHD_NUM", "INTEGER"),
        bigquery.SchemaField("L", "STRING"),
        bigquery.SchemaField("AGE_RANGE", "STRING"),
        bigquery.SchemaField("MARITAL", "STRING"),
        bigquery.SchemaField("INCOME_RANGE", "STRING"),
        bigquery.SchemaField("HOMEOWNER", "STRING"),
        bigquery.SchemaField("HSHD_COMPOSITION", "STRING"),
        bigquery.SchemaField("HH_SIZE", "STRING"),
        bigquery.SchemaField("CHILDREN", "STRING"),
    ],
    'products': [
        bigquery.SchemaField("PRODUCT_NUM", "INTEGER"),
        bigquery.SchemaField("DEPARTMENT", "STRING"),
        bigquery.SchemaField("COMMODITY", "STRING"),
        bigquery.SchemaField("BRAND_TY", "STRING"),
        bigquery.SchemaField("NATURAL_ORGANIC_FLAG", "STRING"),
    ]
}



@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        session['username'] = request.form["username"]
        session['email'] = request.form["email"]
        session['password'] = request.form["password"] 
        return redirect(url_for('sample'))
    return render_template("login.html")


@app.route("/sample", methods=["GET", "POST"])
def sample():
    query = """
        SELECT
            t.HSHD_NUM,
            t.BASKET_NUM,
            SAFE.PARSE_DATE('%d-%b-%y', t.PURCHASE_) AS PURCHASE_DATE,
            t.PRODUCT_NUM,
            p.DEPARTMENT,
            p.COMMODITY,
            t.SPEND,
            t.UNITS,
            t.STORE_R,
            t.WEEK_NUM,
            t.YEAR,
            p.BRAND_TY,
            p.NATURAL_ORGANIC_FLAG,
            h.L,
            h.AGE_RANGE,
            h.MARITAL,
            h.INCOME_RANGE,
            h.HOMEOWNER,
            h.HSHD_COMPOSITION,
            h.HH_SIZE,
            h.CHILDREN
        FROM
            `retail-clv-project.retail_data.transactions` t
        JOIN
            `retail-clv-project.retail_data.products` p
        ON
            t.PRODUCT_NUM = p.PRODUCT_NUM
        JOIN
            `retail-clv-project.retail_data.households` h
        ON
            t.HSHD_NUM = h.HSHD_NUM
        WHERE
            t.HSHD_NUM = 10
        ORDER BY
            t.HSHD_NUM, t.BASKET_NUM, PURCHASE_DATE, t.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY
        LIMIT 20
    """
    query_job = client.query(query)
    results = query_job.result()
    sample_data = [dict(row) for row in results]

    return render_template("sample.html", sample_data=sample_data, username=session['username'])


@app.route("/action", methods=["GET", "POST"])
def action():
    if request.method == "POST":
        hshd_num = request.form.get("hshd_num")
        file_type = request.form.get("file_type")
        csv_file = request.files.get("csv_file")

        if hshd_num and not csv_file:
            session['hshd_num'] = hshd_num
            session['uploaded'] = None
            return redirect(url_for('results'))

        if csv_file and file_type:
            gcs = storage.Client()
            bucket = gcs.bucket("retail-project-bucket11")
            gcs_filename = f"{file_type}_data.csv"
            blob = bucket.blob(gcs_filename)
            blob.upload_from_file(csv_file)

            uri = f"gs://retail-project-bucket11/{gcs_filename}"
            job_config = bigquery.LoadJobConfig(
                schema=schemas[file_type],
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=False,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )

            if file_type == 'transactions':
                table = "retail-clv-project.retail_data.transactions"
            elif file_type == 'households':
                table = "retail-clv-project.retail_data.households"
            else:
                table = "retail-clv-project.retail_data.products"

            load_job = client.load_table_from_uri(uri, table, job_config=job_config)
            load_job.result()

            session['uploaded'] = file_type
            return redirect(url_for('ask_hshd'))

    return render_template("action.html")


@app.route("/ask_hshd", methods=["GET", "POST"])
def ask_hshd():
    if request.method == "POST":
        session['hshd_num'] = request.form["hshd_num"]
        return redirect(url_for('results'))
    return render_template("ask_hshd.html")


@app.route("/results")
def results():
    hshd_num = session.get('hshd_num')
    uploaded = session.get('uploaded')

    if not hshd_num:
        return "No HSHD_NUM provided."

    if uploaded is None:
     query = f"""
        SELECT
            t.HSHD_NUM,
            t.BASKET_NUM,
            PARSE_DATE('%d-%b-%y', t.PURCHASE_) AS PURCHASE_DATE,
            t.PRODUCT_NUM,
            p.DEPARTMENT,
            p.COMMODITY,
            t.SPEND,
            t.UNITS,
            t.STORE_R,
            t.WEEK_NUM,
            t.YEAR,
            p.BRAND_TY,
            p.NATURAL_ORGANIC_FLAG,
            h.L,
            h.AGE_RANGE,
            h.MARITAL,
            h.INCOME_RANGE,
            h.HOMEOWNER,
            h.HSHD_COMPOSITION,
            h.HH_SIZE,
            h.CHILDREN
        FROM
            `retail-clv-project.retail_data.transactions` t
        LEFT JOIN
            `retail-clv-project.retail_data.products` p
        ON
            t.PRODUCT_NUM = p.PRODUCT_NUM
        LEFT JOIN
            `retail-clv-project.retail_data.households` h
        ON
            t.HSHD_NUM = h.HSHD_NUM
        WHERE
            t.HSHD_NUM = {hshd_num}
        ORDER BY
            t.HSHD_NUM, t.BASKET_NUM, PURCHASE_DATE, t.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY
    """

    else:
        if uploaded == 'transactions':
            query = f"""
                SELECT *
                FROM `retail-clv-project.retail_data.transactions`
                WHERE HSHD_NUM = {hshd_num}
                ORDER BY HSHD_NUM, BASKET_NUM, PURCHASE_, PRODUCT_NUM
            """
        elif uploaded == 'households':
            query = f"""
                SELECT *
                FROM `retail-clv-project.retail_data.households`
                WHERE HSHD_NUM = {hshd_num}
            """
        else:  
            return "Products table does not have HSHD_NUM field."

    query_job = client.query(query)
    results = query_job.result()
    data = [dict(row) for row in results]

    if not data:
        return f"No records found for HSHD_NUM = {hshd_num}"

    return render_template("results.html", data=data, username=session['username'], email=session['email'])


if __name__ == "__main__":
    app.run()
