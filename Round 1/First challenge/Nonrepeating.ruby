def first_non_repeating_char(str)
  char_count = {}
  str.each_char do |char|
    if char_count[char]
      char_count[char] += 1
    else
      char_count[char] = 1
    end
  end

  str.each_char do |char|
    return char if char_count[char] == 1
  end

  return nil
end

inputs = [
  "swiss",  
  "hash",  
  "abracadabra",  
  "aabbcc",  
]

inputs.each do |input|
  result = first_non_repeating_char(input)
  if result
    puts "The first non-repeating character in '#{input}' is '#{result}'."
  else
    puts "There is no non-repeating character in '#{input}'."
  end
end