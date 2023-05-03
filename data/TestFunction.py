

func_name_to_match = 'lengthTest'

for i in range(1,4):
        getattr(TestFunctionLib, func_name_to_match)()
        break


def rangeTest(input):
    print('range test is  %s' % (input))


