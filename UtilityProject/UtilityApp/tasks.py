from celery import shared_task
from django.utils import timezone
from .models import ScheduleGroup, ScheduleHistory, PLCConnection
from pymodbus.client import ModbusTcpClient

import time



@shared_task
def execute_schedule(schedule_id, status):
    try:
        # Fetch the schedule
        schedule = ScheduleGroup.objects.get(id=schedule_id)
        if not schedule.is_active:
            print(f"Schedule {schedule.name} is inactive, skipping.")
            return

        # Fetch PLC connection details from the database
        plc = PLCConnection.objects.first()
        if not plc:
            print("No PLC connection found in the database.")
            return

        # Initialize Modbus client with timeout and retries
        timeout = 10  # Default timeout in seconds
        retries = 3   # Default number of retries
        client = ModbusTcpClient(
            host=plc.ip_address,
            port=plc.port,
            timeout=timeout
        )

        # Attempt to connect with retries
        connection_successful = False
        for attempt in range(retries + 1):
            try:
                if client.connect():
                    print(f"Connected to Modbus server at {plc.ip_address}:{plc.port} on attempt {attempt + 1}")
                    connection_successful = True
                    break
                else:
                    print(f"Connection attempt {attempt + 1} failed for {plc.ip_address}:{plc.port}")
            except Exception as conn_error:
                print(f"Connection attempt {attempt + 1} error: {str(conn_error)}")
            if attempt < retries:
                time.sleep(1)  # Wait before retrying

        if not connection_successful:
            print(f"Failed to connect to Modbus server at {plc.ip_address}:{plc.port} after {retries + 1} attempts")
            return

        try:
            # Iterate through tags in the schedule and write to PLC
            for tag in schedule.tags.all():
                try:
                    client.write_coil(int(tag.tag_value), bool(status))
                    tag.status = status
                    tag.save()
                    ScheduleHistory.objects.create(
                        schedule_group=schedule,
                        tag=tag,
                        status=status,
                        user=None,
                    
                    )
                    print(f"Tag {tag.tag_name} set to {'ON' if status else 'OFF'} for schedule {schedule.name}")
                except Exception as tag_error:
                    print(f"Failed to write to tag {tag.tag_name} (value: {tag.tag_value}): {tag_error}")
                    continue  # Continue with the next tag on failure
        finally:
            # Always close the Modbus connection
            if client and client.is_socket_open():
                client.close()
                print("Modbus connection closed after schedule execution")

    except ScheduleGroup.DoesNotExist:
        print(f"Schedule with ID {schedule_id} does not exist.")
    except Exception as e:
        print(f"Error executing schedule {schedule_id}: {e}")