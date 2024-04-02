import json
import threading
import solana
import time
import logging
import numpy as np
import datetime
import csv
from solana.rpc.api import Client

explorerURLAdd = "https://explorer.solana.com/address/"
explorerULRTx = "https://explorer.solana.com/tx/"

Address_RaydiumAMM = "DLq6eNr4iEAbqW5tC6yN2nU12DwSWSLGPjRBVNxcrHXH"
Address_solscan = "https://api.solscan.io/account?address="

lastSignature = None
resultArr = []
processedTxArr = []
rounds = 0
txCount = 0

endpoint = "https://solana-mainnet.core.chainstack.com/ae2b75f218dd717c390ec0d9cda4dba2"
http_client = Client(endpoint, timeout=60)

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
    try:
        txSignature2 = solana.transaction.Signature.from_string(txSignature)
        tx = http_client.get_transaction(txSignature2,max_supported_transaction_version=0)
        tx = json.loads(tx.to_json())
        postTokenBalances = tx["result"]["meta"]["postTokenBalances"]
        preTokenBalances = tx["result"]["meta"]["preTokenBalances"]
        if (postTokenBalances != preTokenBalances):
            resultArr.append(tx)
    except Exception as e:
        logging.error(f"Error occurred while processing transaction {txSignature}: {e}")

if __name__ == "__main__":

    RaydiumPubKey = solana.rpc.types.Pubkey.from_string(Address_RaydiumAMM)
     
    while True:
        try:
            txs = http_client.get_signatures_for_address(RaydiumPubKey, limit=100, before=lastSignature).to_json()
            txs = json.loads(txs)["result"]
            signatures = np.array([o["signature"] for o in txs])

            print(signatures)
            threads = list()
            for signature in signatures:
                txCount += 1
                x = threading.Thread(target=getTxDetail, args=(signature,))
                threads.append(x)
                x.start()
                # Introduce a delay between requests to stay within the rate limit
            for index, thread in enumerate(threads):
                thread.join()

            if not txs:
                break
            else:
                rounds += 1
                lastSignature = solana.transaction.Signature.from_string(txs[-1]["signature"])
            
            time.sleep(3)

        except Exception as e:
            logging.error(f"Error occurred: {e}")

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
