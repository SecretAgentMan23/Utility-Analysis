import Analysis
import sys
import os

def controller(fname):
    if not (os.path.isdir(fname)):
        sys.exit("The folder {} could not be found".format(fname))

    print("What type of analysis would you like to run?")
    print("1. APS")
    print("2. TEP")
    print("3. NVE")
    print("4. SRP")
    option = int(input("Number: "))
    if option == 1:
        try:
            Analysis.APSAnalysis(fname)
        except FileNotFoundError as e:
            sys.exit("The file {} could not be found in the folder {}".format(e, fname))
        except ValueError as e:
            sys.exit("The specified folder '{}' does not have any csv files in it".format(e.args))
        except NotADirectoryError as e:
            sys.exit("The specified folder '{}' could not be found in the folder {}".format(e, fname))
    if option == 2:
        Analysis.TEPAnalysis(fname)
    if option == 3:
        Analysis.NVEAnalysis(fname)
    if option == 4:
        Analysis.SRPAnalysis(fname)

def main():
    controller(sys.argv[1])

if __name__ == "__main__":
    main()



# Move the sys.exit calls to a function inside view.py
# Have a print menu function inside view.py that we call here
#
