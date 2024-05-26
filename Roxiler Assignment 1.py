from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import requests

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///product_transactions.db'
db = SQLAlchemy(app)
ma = Marshmallow(app)

class ProductTransaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200))
    description = db.Column(db.String(500))
    price = db.Column(db.Float)
    category = db.Column(db.String(100))
    date_of_sale = db.Column(db.String(20))
    sold = db.Column(db.Boolean)

class ProductTransactionSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = ProductTransaction

transaction_schema = ProductTransactionSchema()
transactions_schema = ProductTransactionSchema(many=True)

db.create_all()

@app.route('/initialize_db', methods=['GET'])
def initialize_db():
    url = "https://s3.amazonaws.com/roxiler.com/product_transaction.json"
    response = requests.get(url)
    data = response.json()
    
    db.drop_all()
    db.create_all()
    
    for item in data:
        transaction = ProductTransaction(
            title=item['title'],
            description=item['description'],
            price=item['price'],
            category=item['category'],
            date_of_sale=item['dateOfSale'],
            sold=item['sold']
        )
        db.session.add(transaction)
    
    db.session.commit()
    return jsonify({"message": "Database initialized with seed data"}), 200

@app.route('/transactions', methods=['GET'])
def get_transactions():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    search = request.args.get('search', '', type=str)
    query = ProductTransaction.query
    
    if search:
        search = f"%{search}%"
        query = query.filter(
            (ProductTransaction.title.ilike(search)) |
            (ProductTransaction.description.ilike(search)) |
            (ProductTransaction.price.ilike(search))
        )
    
    transactions = query.paginate(page, per_page, error_out=False)
    result = transactions_schema.dump(transactions.items)
    return jsonify(result), 200

@app.route('/statistics', methods=['GET'])
def get_statistics():
    month = request.args.get('month', type=str)
    transactions = ProductTransaction.query.filter(
        ProductTransaction.date_of_sale.like(f"%{month}%")
    ).all()
    
    total_sale_amount = sum(t.price for t in transactions if t.sold)
    total_sold_items = sum(1 for t in transactions if t.sold)
    total_not_sold_items = sum(1 for t in transactions if not t.sold)
    
    return jsonify({
        "total_sale_amount": total_sale_amount,
        "total_sold_items": total_sold_items,
        "total_not_sold_items": total_not_sold_items
    }), 200

@app.route('/bar_chart', methods=['GET'])
def get_bar_chart():
    month = request.args.get('month', type=str)
    transactions = ProductTransaction.query.filter(
        ProductTransaction.date_of_sale.like(f"%{month}%")
    ).all()
    
    price_ranges = {
        "0-100": 0,
        "101-200": 0,
        "201-300": 0,
        "301-400": 0,
        "401-500": 0,
        "501-600": 0,
        "601-700": 0,
        "701-800": 0,
        "801-900": 0,
        "901-above": 0
    }
    
    for t in transactions:
        price = t.price
        if price <= 100:
            price_ranges["0-100"] += 1
        elif price <= 200:
            price_ranges["101-200"] += 1
        elif price <= 300:
            price_ranges["201-300"] += 1
        elif price <= 400:
            price_ranges["301-400"] += 1
        elif price <= 500:
            price_ranges["401-500"] += 1
        elif price <= 600:
            price_ranges["501-600"] += 1
        elif price <= 700:
            price_ranges["601-700"] += 1
        elif price <= 800:
            price_ranges["701-800"] += 1
        elif price <= 900:
            price_ranges["801-900"] += 1
        else:
            price_ranges["901-above"] += 1
    
    return jsonify(price_ranges), 200

@app.route('/pie_chart', methods=['GET'])
def get_pie_chart():
    month = request.args.get('month', type=str)
    transactions = ProductTransaction.query.filter(
        ProductTransaction.date_of_sale.like(f"%{month}%")
    ).all()
    
    category_counts = {}
    
    for t in transactions:
        category = t.category
        if category in category_counts:
            category_counts[category] += 1
        else:
            category_counts[category] = 1
    
    return jsonify(category_counts), 200

@app.route('/combined_data', methods=['GET'])
def get_combined_data():
    month = request.args.get('month', type=str)
    
    transactions_response = get_transactions()
    statistics_response = get_statistics()
    bar_chart_response = get_bar_chart()
    pie_chart_response = get_pie_chart()
    
    combined_response = {
        "transactions": transactions_response.get_json(),
        "statistics": statistics_response.get_json(),
        "bar_chart": bar_chart_response.get_json(),
        "pie_chart": pie_chart_response.get_json()
    }
    
    return jsonify(combined_response), 200

if __name__ == '__main__':
    app.run(debug=True)