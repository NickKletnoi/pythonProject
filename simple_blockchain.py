import hashlib

"""
Represents a block inside a blockchain.
"""
class Block:
	# Properties
	# Initialization
	def __init__(self, previous_block_hash, transaction):
		self.previous_block_hash = previous_block_hash
		self.transaction = transaction
		self.hash = self.create_hash()

	# Methods
	def create_hash(self):
		return hashlib.sha256(self.transaction.encode()).hexdigest()

class Blockchain:
	def __init__(self):
		self.chain = []
		self.create_genesis_block()

	def create_genesis_block(self):
		genesis_block = Block("0", "No transaction")
		self.chain.append(genesis_block)

	def add_block(self, transaction):
		previous_block = self.chain[-1]
		previous_block_hash = previous_block.hash
		new_block = Block(previous_block_hash, transaction)
		self.chain.append(new_block)

	def description(self):
		for i in range(len(self.chain)):
			block = self.chain[i]
			print(f"Block index: {i}")
			print(f"Current Hash: {block.hash}")
			print(f"Previous Hash: {block.previous_block_hash}")
			print(f"Transaction : {block.transaction}")
			print("")

blockchain = Blockchain()
blockchain.add_block("Annie sent Braum 20 Bttercoins")
blockchain.add_block("Braum sent Caitlyn 10 Bttercoins")
blockchain.description()
