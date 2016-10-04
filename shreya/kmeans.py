def kmeans(points_with_weights):
	#Based on the algorithm given here: http://cs-people.bu.edu/lapets/591/s.php#cba5543907854ed28dbd3eeb874ebd54
	#Chose 45 means because that's approximately how many zipcodes Boston has.
	#This point is what is generated for the zipcode 02215.
	M = [(42.3457429616475,-71.1025665422545)]*45
	P = list(points_with_weights.keys())
	def dist(p,q):
		(x1,y1) = p
		(x2,y2) = q
		return (x1-x2)**2 + (y1-y2)**2

	def plus(args):
		p = [0,0]
		for (x,y) in args:
			p[0] +=x
			p[1] +=y
		return tuple(p)

	def scale(p,c):
		(x,y) = p
		return (x/c, y/c)

	def product(R, S):
		return [(t,u) for t in R for u in S]
	def aggregate(R, f):
		keys = {r[0] for r in R}
		return [(key, f([v for (k,v) in R if k == key])) for key in keys]

	count = 0
	#In the future gonna check to make sure the distance is under a certain amount.
	while count < 10:
		MPD = [(m, p, dist(m,p)) for (m,p) in product(M,P)]
		PDs = [(p, d) for (m,p,d) in MPD]
		PD = aggregate(PDs, min) 
		MP = [(m,p) for ((m,p,d),(p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
		MT = aggregate(MP, plus)
		#Instead of 1 put salaries as the weight.
		MWeight = [(m, points_with_weights[p]) for ((m, p, d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
		MC = aggregate(MWeight, sum)

		M = [scale(t,c) for ((m,t), (m2,c)) in product(MT, MC) if m==m2]
		M = sorted(M)
		count+=1
	return M