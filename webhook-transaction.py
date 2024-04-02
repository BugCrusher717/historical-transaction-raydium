import json
import solana
import time
import logging
import threading
import numpy as np
import urllib.request
import json
import datetime
import asyncio
import websockets
import csv
from urllib.request import Request, urlopen

from solana.rpc.api import Client

explorerURLAdd = "https://explorer.solana.com/address/"
explorerULRTx = "https://explorer.solana.com/tx/"

Address_RaydiumAMM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
Address_liquidityPool = "HDkUU1Lr47wRUjaE1LRLniJYwrL11H7956ofcNpLf3uW"
Address_solscan = "https://api.solscan.io/account?address="

lastSignature = None
resultArr=[]
processedTxArr = []
rounds = 0
txCount = 0 
maxTxCount = 1
endpoint = "https://solana-mainnet.core.chainstack.com/ae2b75f218dd717c390ec0d9cda4dba2"
http_client = Client(endpoint)
processTxArr = []

# Define the field names for the CSV
field_names = ["Signature", "Address", "TimeStamp", "Swap Datas", 'fee']

# Assuming you have a filename where you want to save the CSV
filename = "output.csv"

def processTx(tx):
    txSignature = tx["result"]["transaction"]["signatures"][0]
    txSender = tx["result"]["transaction"]["message"]["accountKeys"][0]
    blockTime = tx["result"]["blockTime"]
    slot = tx["result"]["slot"]
    fee = tx["result"]["meta"]['fee']
    postTokenBalances = tx["result"]["meta"]["postTokenBalances"]
    preTokenBalances = tx["result"]["meta"]["preTokenBalances"]
    tokenBalances = []

    for pre, post in zip(preTokenBalances, postTokenBalances):
        change = 0
        if not (post["uiTokenAmount"]["uiAmount"] is None or pre["uiTokenAmount"]["uiAmount"] is None):
            change = post["uiTokenAmount"]["uiAmount"] - pre["uiTokenAmount"]["uiAmount"]
        owner = pre["owner"]
        token = pre["mint"]
        if(change != 0):
            tokenBalances.append({
                "owner":owner,
                "token":token,
                "change":change
            })

    if tokenBalances:
        fee = fee / 1000000000
        formatted_result = "{:.10f}".format(fee).rstrip('0').rstrip('.')
        result = {
            "txSignature":txSignature,
            "sender" : txSender,
            "blockTime" : blockTime,
            "slot" : slot,
            "fee":formatted_result,
            "tokenBalances" : tokenBalances
        }
        return result
    else:
        return None

def getTxDetail(txSignature):
    txSignature2 = solana.transaction.Signature.from_string(txSignature)
    tx = http_client.get_transaction(txSignature2,max_supported_transaction_version=0)
    tx = json.loads(tx.to_json())
    if tx["result"] :
        postTokenBalances = tx["result"]["meta"]["postTokenBalances"]
        preTokenBalances = tx["result"]["meta"]["preTokenBalances"]
        if (postTokenBalances != preTokenBalances):
            resultArr.append(tx)


async def handle_swap_notification(message):
    notification = json.loads(message)
    if 'params' in notification:
        result = notification["params"]["result"]
    else:
        result = notification["result"]

    if isinstance(result, dict) and "value" in result and isinstance(result["value"], dict) and "signature" in result["value"]:
        txSignature = result["value"]["signature"]
        print(result["value"]["signature"])
        getTxDetail(txSignature)
        await save_to_csv()


async def connect_to_websocket():
    while True:
        try:
            uri = "wss://solana-mainnet.core.chainstack.com/ws/ae2b75f218dd717c390ec0d9cda4dba2"
            async with websockets.connect(uri) as websocket:
                await websocket.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": [{"mentions": [Address_RaydiumAMM]}]}))
                while True:
                    message = await websocket.recv()
                    await handle_swap_notification(message)
        except Exception as e:
            print("WebSocket connection error:", e)
            print("Attempting to reconnect in 5 seconds...")
            await asyncio.sleep(5)

async def main():
    await connect_to_websocket()

async def save_to_csv():
    global processedTxArr
    global field_names
    global filename
    
    if resultArr:
        for tx in resultArr:
            processed_tx = processTx(tx)
            if processed_tx:
                processedTxArr.append(processed_tx)

        processedTxArr_copy = processedTxArr[:]  # Make a copy to avoid race conditions
        
        normalisedTxArr_copy = []
        # Normalize transaction data
        for tx in processedTxArr_copy:
            date = datetime.datetime.utcfromtimestamp(tx["blockTime"])
            normalisedTxArr_copy.append({
                'Signature': tx["txSignature"],
                'Address': tx["sender"],
                'TimeStamp': date,
                'Swap Datas': tx["tokenBalances"],
                'fee':tx["fee"]
            })

        # Write to CSV
        with open(filename, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            
            # Write each transaction to CSV
            for data in normalisedTxArr_copy:
                writer.writerow(data)
        
        resultArr.clear()
        processedTxArr.clear()

if __name__ == "__main__":
    RaydiumPubKey = solana.rpc.types.Pubkey.from_string(Address_RaydiumAMM)

    asyncio.run(main())

    while True:
        
        time.sleep(10)
        txs = http_client.get_signatures_for_address(RaydiumPubKey,limit=200,before=lastSignature).to_json()
        txs = json.loads(txs) ["result"]
        signatures = np.array([o["signature"] for o in txs])

        threads = list()
        for signature in signatures:
            txCount += 1
            x = threading.Thread(target=getTxDetail, args=(signature,))
            threads.append(x)
            x.start()
            
        for index, thread in enumerate(threads):
            thread.join()

        if(txCount >= maxTxCount):
            break
        else:
            rounds += 1
            lastSignature = solana.transaction.Signature.from_string(txs[-1]["signature"])
        
        time.sleep(3)
    
    for tx in resultArr:
        processed_tx = processTx(tx)
        if processed_tx:
            processedTxArr.append(processed_tx)
 
    normalisedTxArr = []

    for tx in processedTxArr:
        date = datetime.datetime.fromtimestamp(tx["blockTime"], tz=datetime.timezone.utc)
        normalisedTxArr.append({
            'Signature': tx["txSignature"],
            'Address': tx["sender"],
            'TimeStamp': date,
            'Swap Datas': tx["tokenBalances"],
            'fee':tx["fee"]
        })

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)

        # Write the header
        writer.writeheader()

        # Write the data
        for data in normalisedTxArr:
         writer.writerow(data)
    # print(normalisedTxArr)
         
    