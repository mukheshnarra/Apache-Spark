# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 13:12:40 2019

@author: MUKHESH
"""
from flask import render_template,jsonify,Flask,request
import ast
app=Flask(__name__)
labels=[]
values=[]
@app.route('/')
def get_chart_data():
    global labels,values
    return render_template('chart.html',labels=labels,values=values)
    
@app.route('/refresh')
def refresh_data():
    global labels,values
    return jsonify(sLabels=labels,sValues=values)

@app.route('/update',methods=['POST'])
def update_data():
    global labels,values
    if not request.form: 
        return "error",400
    temp=request.form['labels']
    temp=[str(p) for p in temp[5:-1].split(',')]
    temp_1=request.form['values']
    temp_1="["+temp_1[5:-1]+"]"
#    labels=ast.literal_eval(temp)
    labels=temp
    values=ast.literal_eval(temp_1)
#    print(labels[0],values)
#    print(temp,temp_1)
    return "success",201

if __name__=="__main__":
    app.run(host="localhost",port=5000)

















