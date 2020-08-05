## STEPS
  
- Check out this Repo

- Install the dependencies with 'pip install -r requirements.txt'

- Write your new node in .../python/fire/customnode

- Test it by writing a workflow similar to workflow_csv.py


- Drop your new node into Fire Installation at .../fire-3.1.0/dist/fire_custom

- Create the new jobs_custom.zip with 'zip -r jobs_custom.zip fire_custom'

- Create the json file for your new node

- Drop the json files into nodes/etl

- Restart Fire Server with ./run-fire-server.sh start

