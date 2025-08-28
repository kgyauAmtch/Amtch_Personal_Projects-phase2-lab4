from validate import run_validation
from archive import run_archive

def lambda_handler(event, context):
    action = event.get("action")

    if action == "validate":
        return run_validation()
    elif action == "archive":
        return run_archive(
            source_bucket=event.get("source_bucket"),
            folders=event.get("folders", [])
        )
    else:
        return {
            "statusCode": 400,
            "message": f"Unknown action: {action}"
        }
