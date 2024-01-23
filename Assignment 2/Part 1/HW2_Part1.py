#Lets read the input file:

with open("input_part1_and_part2.txt", 'r') as file:
    #Read the contents of the file
    file_contents = file.read()
    #Split the contents into a list of rows
    input_file = file_contents.splitlines()

#print(input_file)

#Lets create a dictionary with all the keys and set values to 0 at first    
dictionary = {'Chicago':0, 'Dec':0, 'Java':0, 'Hackathon':0, 'Engineers':0}

#Lets see the file by rows and count the number of occurances for each of the keys
for i in input_file:
    print(i.lower())
    for j in dictionary:
        if j.lower() in i.lower():
            dictionary[j]+=1

#print(dictionary)
print('\n\n')

#Lets see the final count as shown in the task
for i in dictionary:
    print(i, dictionary[i])
    
