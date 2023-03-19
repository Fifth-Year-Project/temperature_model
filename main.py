import requests
import pandas as pd
import numpy as np
import csv
import os
import lumen
import traceback
import json
import sys




def poll_temp_sensors(number_of_readings):
    headers = {'Content-Type': 'application/x-www-form-urlencoded',}
    data = '{"access_key": "5bdd8b2f-fa79-4620-8dfe-9c91becf87f2", "limit": %s}' %number_of_reading
    response = requests.post('https://sdls.lumen.live/documents/external/filter', headers=headers, data=data)
    
    temperature_info = []
    for i in range(number_of_reading):
        temp_data = response.json()[i]["execution-results"]["results"]
        for data in temp_data:
            temperature_info.append(data)
    
    return(temperature_info)




def rise_fall_detection(temperatrue_frame_dic):

    Temperature = list(temperatrue_frame_dic.values())      #List holding the temperature of each reading
    #with open(os.path.join(_location_, "Input.csv"), "rb" ) as f:
    #    Temperature = pd.read_csv(f)
    #    Temperature = list(Temperature["Temperatrue"])
    
    
    Time = list(temperatrue_frame_dic.keys())               #List holding the epoch of each reading
    index_array = []                                        #List holding an idex to the number of readings
    for i in range(0, len(Temperature)):
        index_array.append(i)
    
    Chunk_size = 24                     #The number of readings to be compared at once
    Constraint_threshold = 1.5          #This may need tuned for the new application

    state = 0
    Rise_or_fall = []

    for i in range(0, len(Temperature)):    #For each reading
        if i <= (len(Temperature)-Chunk_size) and i >= (Chunk_size):               #Ignore the first 12 readings NOT SURE THIS IS RIGHT NUMBER
            #Holds the i'th temperature and chunk_size additional temperature 
            Temp_values = Temperature[i:i+Chunk_size] #THE i'th TEMP IS THE LAST VALUE 
        
            positive = []
            negative = []
            ignore = []
            changeintemp = []
            State_changes = []

            counter2 = 0
            counter3 = 0
            positive_sum = 0
            negative_sum = 0

            while counter2 < (len(Temp_values)-1):      #For the length of chunk_size
                tempchange = (Temp_values[counter2 + 1] - Temp_values[counter2])    #Difference between two temepratures 
                changeintemp.append(tempchange)
                
                counter2 = counter2 + 1

            while counter3 < (len(Temp_values)-1):      #For the length of chunk_size
                if changeintemp[counter3] > 0:          #If the current change in temp is +Ve
                    positive.append(changeintemp[counter3]) #Add to positive
                if changeintemp[counter3] < 0:          #If the current change in temp is -Ve
                    negative.append(changeintemp[counter3]) #Add to negative
                if changeintemp[counter3] == 0:         #If there is no change in temp
                    ignore.append(changeintemp[counter3])   #Ignore 
                
                counter3 = counter3 + 1


            for i in range(0, len(positive)):           #For all the postive changes 
                positive_sum = positive_sum + positive[i]   #Sum
                
                
            for i in range(0, len(negative)):           #For all the negative changes
                negative_sum = negative_sum + negative[i]   #Sum

            negative_sum = negative_sum * -1            #Make negative sum positive for comparrison

            #If the postive change is greater than the negative change
            #and the postive change is greater by a large enough margin to exceed the constraint threshold 
            if positive_sum > negative_sum and positive_sum > (negative_sum*(Constraint_threshold)):
                state = 1   #state 1 = rising
                
            #If the negative change is greater than the negative change
            #and the negative change is greater by a large enough margin to exceed the constraint threshold     
            if negative_sum > positive_sum and negative_sum > (positive_sum*(Constraint_threshold)):
                state = 0   #state 0 = falling
                

            #print("The positive sum is",positive_sum)
            #print("The negative sum is",negative_sum)

            if state == 1:
                #print("The temp is rising")
                a=1
            if state == 0:
                #print("The temp is falling")
                a=1
            
            Rise_or_fall.append(state) 

            for i in range(0,len(Rise_or_fall)):                    #For all chunks currently evaluated
                if i > 1 and Rise_or_fall[i] != Rise_or_fall[i-1]:  #If the current chunks state changes from the previous state
                    #Add to details to changes of the point where the turning point is found
                    Changes = [Temperature[i-Chunk_size],Time[i-Chunk_size], Rise_or_fall[i],index_array[i-Chunk_size]]  
                    State_changes.append(Changes)

    #print (State_changes)

    return State_changes
    

def fit_line(index_val,temperature_array):
    cmin, cmax = min(index_val), max(index_val)
    pfit, stats = np.polynomial.Polynomial.fit(index_val, temperature_array, 1, full=True, window=(cmin, cmax),
                                                    domain=(cmin, cmax))

    c, m = pfit
    resid, rank, sing_val, rcond = stats
    rms = np.sqrt(resid[0]/len(temperature_array))

    print('Fit: Temp = {:.3f}*x + {:.3f}'.format(m, c),
        '(rms residual = {:.4f})'.format(rms))

    return m,c



#Main code flow 
try:
    
    _location_ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


    number_of_reading = 150
    
    
    
    temperature_frame = json.loads(sys.argv[2])
    
    temperature_frame = poll_temp_sensors(number_of_reading)    #Data from Lumen 
    df = pd.DataFrame(temperature_frame)                        #Convert to data frame

    #Dictionary with the time and temperature
    temperature_frame_dic = dict(map(lambda i,j : (i,j) , list(df["epoch"]),list(df["temperature"]))) 

    State_changes = rise_fall_detection(temperature_frame_dic)

    no_state_changes = len(State_changes)

    rising_temperature_array = []
    rising_index_array = []
    falling_temperature_array = []
    falling_index_array = []


    #temp_temperature_array = list(temperature_frame_dic.values())
    with open(os.path.join(_location_, "Input.csv"), "rb" ) as f:
            temp_temperature_array = pd.read_csv(f)
            temp_temperature_array = list(temp_temperature_array["Temperatrue"])


    temp_index_array = []
    for i in range(0,len(temp_temperature_array)):
        temp_index_array.append(i)

    for i in range(no_state_changes-1):         #For each of the state changes (except the last as we are in this cycle)
            temp = []
            if State_changes[i][2] == 1:       #If the current state change is a minimum
                rising_temperature_array.append(temp_temperature_array[State_changes[i][3]:State_changes[i+1][3]-1])
                for ii in range((State_changes[i+1][3]-1)-State_changes[i][3]):
                    counterxxx = ii + State_changes[i][3]
                    temp.append(temp_index_array[counterxxx])
                rising_index_array.append(temp)   
                
            elif State_changes[i][2] == 0:     #If the current state change is a maximum
                falling_temperature_array.append(temp_temperature_array[State_changes[i][3]:State_changes[i+1][3]-1])
                for ii in range((State_changes[i+1][3]-1)-State_changes[i][3]):
                    counterxxx = ii + State_changes[i][3]
                    temp.append(temp_index_array[counterxxx])
                falling_index_array.append(temp)   
        
    #Output the rising temperature data to excel
    with open('Rising.csv', 'w') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        for row in zip((rising_temperature_array,rising_index_array)):
            wr.writerow(row)  
    #Output the falling temperature data to excl
    with open('Falling.csv', 'w') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        for row in zip((falling_temperature_array,falling_index_array)):
            wr.writerow(row)    
    #Output the full temperature data to excel
    with open('Whole_Temp.csv', 'w') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        for row in zip(temp_temperature_array):
            wr.writerow(row) 
        
    #Fit the rise and fall data without any clensing 
    falling_m_array = []
    falling_c_array = []

    for i in range(len(falling_index_array)):
        m,c=fit_line(falling_index_array[i],falling_temperature_array[i]) 
        falling_c_array.append(c)
        falling_m_array.append(m)

    falling_c = sum(falling_c_array)/len(falling_c_array)
    falling_m = sum(falling_m_array)/len(falling_m_array)
    
    rising_m_array = []
    rising_c_array = []
    
    for i in range(len(rising_index_array)):
        m,c=fit_line(rising_index_array[i],rising_temperature_array[i]) 
        rising_c_array.append(c)
        rising_m_array.append(m)

    rising_c = sum(rising_c_array)/len(rising_c_array)
    rising_m = sum(rising_m_array)/len(rising_m_array)
    

    print('Fit: Falling_Temp = {:.4f}*x + {:.3f}'.format(falling_m, falling_c))
    print('Fit: Rising_Temp = {:.4f}*x + {:.3f}'.format(rising_m, rising_c))

    #Fit the rise and fall data post clensing 
    clensed_falling_m_array = []
    clensed_falling_c_array = []

    for i in range(len(falling_index_array)):
        if len(falling_index_array[i]) >= 100:
            m,c=fit_line(falling_index_array[i][int(len(falling_index_array[i])*0.1):int(1-0.1*len(falling_index_array[i]))]
    ,falling_temperature_array[i][int(len(falling_temperature_array[i])*0.1):int(1-0.1*len(falling_temperature_array[i]))]) 
            clensed_falling_c_array.append(c)
            clensed_falling_m_array.append(m)

    clensed_falling_c = sum(clensed_falling_c_array)/len(clensed_falling_c_array)
    clensed_falling_m = sum(clensed_falling_m_array)/len(clensed_falling_m_array)
    
    clensed_rising_m_array = []
    clensed_rising_c_array = []
    
    for i in range(len(rising_index_array)):
        if len(rising_index_array[i]) >= 100:
            m,c=fit_line(rising_index_array[i][int(len(rising_index_array[i])*0.1):int(1-0.1*len(rising_index_array[i]))]
        ,rising_temperature_array[i][int(len(rising_temperature_array[i])*0.1):int(1-0.1*len(rising_temperature_array[i]))]) 
            clensed_rising_c_array.append(c)
            clensed_rising_m_array.append(m)

    clensed_rising_c = sum(clensed_rising_c_array)/len(clensed_rising_c_array)
    clensed_rising_m = sum(clensed_rising_m_array)/len(clensed_rising_m_array)
    

    print('Fit: Clensed_Falling_Temp = {:.4f}*x + {:.3f}'.format(clensed_falling_m, clensed_falling_c))
    print('Fit: Clensed_Rising_Temp = {:.4f}*x + {:.3f}'.format(clensed_rising_m, clensed_rising_c))

    lumen.save({"clensed_falling_m": clensed_falling_m,"clensed_falling_c":clensed_falling_c,
               "clensed_rising_m":clensed_rising_m,"clensed_rising_c":clensed_rising_c})

    
except Exception as e:
    lumen.save_exception(traceback.format_exc())

