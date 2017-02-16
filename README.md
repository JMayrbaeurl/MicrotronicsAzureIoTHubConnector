# Microtronics Azure IoT Hub ConnectorThis is a sample showing how to connect and integrate [Microtronics rapidM2M Backend ](https://www.microtronics.at/en/products/m2m-platform.html)service to [Azure IoT Hub](https://azure.microsoft.com/en-us/services/iot-hub/).![alt text](./Assets/IntegrationArchitecture.PNG)### How it worksUsing a time triggered [Azure Function App](https://azure.microsoft.com/en-us/services/functions/) (Azure's serverless implementation) the Microtronics m2m Backend middleware is called regularly (default setting is once a minute) via its REST API by using the 'Site's time series data' method to retrieve the newest available telemetry data. Polling requests and their results get stored in an [Azure DocumentDB](https://azure.microsoft.com/en-us/services/documentdb/) collection and are finally forwarded to Azure IoT Hub.  ### Installation You will need the following components to use or run this sample:1. Azure subscription (You can get one for free [here](https://azure.microsoft.com/en-us/free/))2. Visual Studio 2015 (at least Community edition) with Git support3. Access to a Microtronics M2M Backend instance ([playground works too](https://www.microtronics.at/en/service/cloud_service.html))### Configuration- Scheduling for the Function App time trigger can be found in the file located at .\rapidM2MPoller\rapidM2MForwarding\function.json. For more details on configuring Time triggered Azure functions see the documentation on the [Azure website](https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-timer).- Application configuration can be found in the file .\rapidM2MPoller\application.json.
    - M2MBackendURL
    - Base64encodedUsernamePassword
    - Customer_ID
    - Site_ID
    - EmulationMode
    - M2MDocumentsDBUri
    - M2MDocumentsDBPrimaryKey
    - IoTHubConnection
