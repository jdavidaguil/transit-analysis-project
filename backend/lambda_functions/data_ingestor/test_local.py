import json
from lambda_function import lambda_handler

def test_lambda_locally():
    # Simulate Lambda event and context
    test_event = {}
    test_context = None
    
    # Call the lambda handler
    response = lambda_handler(test_event, test_context)
    
    # Print the response
    print("Lambda Response:")
    print(json.dumps(response, indent=2))

if __name__ == "__main__":
    test_lambda_locally()