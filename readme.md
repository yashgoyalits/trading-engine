This is the trading system 
Async Event-Driven Pub/Sub queue based

main initialize 
• logger
• event bus (central hub)
• broker (data provider)
• data manager
    takes data and build candle based on users req and send to hub
• storage manager
• strategy manager wire all strategies
    uses strategy config 
• order placement manager


main = wires the whole system
strategy manager = wires the strategies
strategies = run the logic
order manager = sends orders
hub = routes everything
broker = provides data
storage = records everything
