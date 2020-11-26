sandbox
----
Sandbox implements a simple order driven limit order book (LOB) and exchange simulator. Trading systems may connect to the exchange via the [SOE](../eridanus/readme.md) protocol and receive market data via the [SMD](../fornax/readme.md) protocol.

#### Usage
 The `app.py` simulator application uses configuration file to setup initial state for the simulation and to control the order flow generation. The parameters that control the simulation, are defined in the configuration file in the `src/etc` directory in the `config.ini` file. Make sure these are properly defined before running the simulation.
 
 In order to run the application, open terminal in the project root directory and simply run: 
```python
python app/main.py
```
When started, the exchange simulator initializes the LOB according to the parameters defined in the `config.ini` file.
After initialization, it begins to generate orders based on its model.

#### Configuration

```python
[market-data]
request-address = 127.0.0.1  
request-port    = 5000

[order-entry]
request-address = 127.0.0.1
request-port    = 7001

[book]
initial-best-bid     = 99
initial-best-ask     = 100
initial-levels       = 20
initial-orders       = 10
initial-order-volume = 10


[display]
style = MESSAGE # or BOOK
```
