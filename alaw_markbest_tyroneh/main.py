import sys
from getData import getData
from transformData import transformData
from optimizeBusAllocation import optimizeBusAllocation
from optimizeBusStops import optimizeBusStops
from mapData import mapData

def main():
	help_str = 'main.py accepts 2 arguments: -t trial mode -v visualization'
	t = False
	v = False
	if(len(sys.argv[1:]) > 0):
		for a in sys.argv[1:]:
			if a in ('-h','--help'):
				print(help_str)
				sys.exit()
			else:
				if a in ("-t"):
					print('Trial Mode')
					t = True
				if a in ("-v"):
					print('Graphs Enabled')
					v = True
	else:
		print('Error: ' + help_str)
		sys.exit()

	getData.run(trial = t)
	transformData.run()
	# if(v):
	# 	mapData.visualize()
	optimizeBusAllocation.run(trial=t, visual = v)
    optimizeBusStops.run(trial=t)

if __name__ == '__main__':
	main()
