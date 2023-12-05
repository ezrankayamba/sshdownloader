import sys
import traceback


def print_stack_trace():
    print("-"*60)
    traceback.print_exc(file=sys.stdout)
    print("-"*60)
