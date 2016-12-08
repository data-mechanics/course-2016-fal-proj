class vector:
    def __init__(self, x, y, xFirst=True):
        if xFirst:
            self.x = x
            self.y = y
        else:
            self.x = y
            self.y = x

    @staticmethod
    def dist(u, v, squared=True):
        d = (v.x-u.x)**2 + (v.y-u.y)**2
        if not squared: 
            d = d ** 0.5
        return d

    def __add__(self, other):
        return vector(self.x + other.x, self.y + other.y)

    def __sub__(self, other):
        return vector(self.x - other.x, self.y - other.y)

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y

    def __str__(self):
        return "v(" + str(self.x) + ", " + str(self.y) + ")"

    def __repr__(self):
        return self.__str__()

    def tup(self):
        return (self.x, self.y)

    def scale(self, c):
        ''' Returns a vector scaled by constant c '''
        return vector(c*self.x, c*self.y)

    def dot(self, v):
        return self.x * v.x + self.y * v.y

    def mag(self, squared = True):
        d = self.x*self.x + self.y*self.y
        if not squared:
            d = d ** 0.5
        return d

    def project(self, v):
        '''projects this vector onto vector v'''

        c = self.dot(v) / v.mag()
        return v.scale(c)

    def normal(self, v):
        p = self.project(v)

        norm = p - self

        in_xbounds = (v.x >= 0 and 0 <= p.x <= v.x) or (v.x <= 0 and v.x <= p.x <= 0)
        in_ybounds = (v.y >= 0 and 0 <= p.y <= v.y) or (v.y <= 0 and v.y <= p.y <= 0)

        if not (in_xbounds and in_ybounds):
            # find the closest endpoint of the line to the original point
            cl = min(vector(0, 0), v, key=lambda x: vector.dist(x, self))
            norm = cl - self
            #cl_x, cl_y = min((0, 0), v, key=lambda x: dist(x, u))
            #norm = (cl_x - ux, cl_y - uy)

        return norm