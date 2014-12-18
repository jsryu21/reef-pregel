
- Program Build

:~/reef-pregel$ mvn clean install

- Program Execution

:~/reef-pregel/bin$ ./run.sh -input file:///~/reef-pregel/bin/dataset

(if you run this program on hadoop, use this option : -local false)

- how to verify output
:$ grep -r "R_E_S_U_L_T" ~/reef-pregel/bin/REEF_LOCAL_RUNTIME 


