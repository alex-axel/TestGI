import os
from flask import Flask, send_from_directory, abort

app = Flask(__name__)

@app.route("/techquest/<filename>")
def get_file(filename):
    try:
        return send_from_directory(
            os.path.join(os.getcwd(), 'gi_python_dwh/reports'), 
            filename=filename, 
            as_attachment=True
        )
    except FileNotFoundError:
        abort(404)

if __name__ == '__main__':
    os.environ['FLASK_DEBUG'] = '1'
    app.run(debug=True)