from flask import Flask, render_template

app = Flask(__name__)

n=0

@app.route("/")
def home():
    global n

    # while 1:
    #     n+=1
    #     print(n)

    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)