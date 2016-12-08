

def convert(district):
    '''takes in a string district of the form LetterNumber and returns
a string zipcode'''
    if district in ['A1','A15']:
        return '02215'
    #districts A1 and A15 = downtown and charlestown
    elif district =='A7':
        return '02128'
    #district a7 = east boston
    elif district == 'B3':
        return '02124'
    #district b3 = mattapan
    elif district == 'C6':
        return '02127'
    #district c6 = southie
    elif district == 'C11':
        return '02122'
    #district c11 = dorchester
    elif district == 'D4':
        return '02116'
    #district d4 = south end
    elif district == 'D14':
        return '02135'
    #district d14 = brighton
    elif district == 'E5':
        return '02135'
    #district e5 = west roxbury
    elif district == 'E13':
        return '02130'
    #district e13 = jamaica plain
    elif district == 'E18':
        return '02136'
    #district e18 = hyde park
    else:
        return 'UNEXPECTED DISTRICT ' + district
