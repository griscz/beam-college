import gmpy2

def sparkles_for_number(number): 
  if number == 1:
    return '✧･ﾟ'
  if gmpy2.is_prime(number):
    return '❀･ﾟ✧･ﾟ'

  # None is a funnier return value than an empty string, ok?
  return None 

def add_sparkles(word, number):
  sparkles = sparkles_for_number(number)
  if sparkles:
    return '%s%s%s: %d' % (sparkles, word, sparkles[::-1], number)
  return '%s: %d' % (word, number)
