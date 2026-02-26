# Deploying the ADF Pipeline Debugger as a Logic App Autonomous Agent

The Autonomous AI Agent feature in Azure Logic Apps enables us to orchestrate the pipeline debugging workflow entirely within the Azure cloud, using native connectors. 

Instead of Python orchestrating the requests, the Azure Logic App "Agent Loop" will **Think, Act, and Learn**, utilizing our Python backend as a robust **Tool** to search the error knowledge base.

## Prerequisites
- An active Azure Logic Apps (Standard) or Consumption (preview) workspace.
- An Azure OpenAI connection configured in your Logic App.
- The ADF Pipeline Debugger Dashboard deployed to Azure App Service (to serve as the `/api/agent_search` tool).

## Step 1: Create the Logic App Workflow
1. In the **Azure Portal**, go to your Logic App resource.
2. Under **Workflows**, select **Add**. 
3. Name your workflow (e.g., `ADF-Agent-Debugger`) and choose the **Autonomous Agents** template (or a blank state and add an `Agent` action).
4. For the Trigger, you can use **When an HTTP request is received** or a native **Azure Monitor Alerts** trigger that fires when an ADF pipeline fails.

## Step 2: Configure the AI Model
1. In the designer, click on your **Agent** action (often named "Default Agent").
2. Select **Model** and connect your Azure OpenAI Service. Choose an advanced model like `gpt-4o` for best reasoning results.

## Step 3: Add Instructions
1. Open `agent_instructions.txt` from this repository.
2. Copy the contents and paste them into the **Instructions for agent** text box. This instructs the model on its persona and the specific tasks it must perform.

## Step 4: Import the Vector Knowledge Base Tool
1. Inside the Agent loop, click the **+ (Add tool)** button.
2. We need to create a custom HTTP tool that calls our Python backend. Choose an HTTP action or use the **OpenAPI** importer.
3. If using an OpenAPI importer, point it to the `/api/openapi.json` endpoint running on your deployed Web App (e.g., `https://<your-app-name>.azurewebsites.net/api/openapi.json`).
4. Alternatively, build an HTTP action tool named `SearchKnowledgeBase`:
   - **Method**: `POST`
   - **URI**: `https://<your-app-name>.azurewebsites.net/api/agent_search`
   - **Body**: `{ "query": "@triggerBody()?['error_message']" }` (or dynamically pass the LLM's query argument).
5. Name the tool clearly, for example: `SearchKnowledgeBase`. Make sure its description tells the AI: *"Use this tool to search ChromaDB for similar past ADF errors and their known solutions."*

## Step 5: Add the Email Tool
1. Add another tool inside the Agent loop. Search for the **Send an email (V2)** action (Office 365 Outlook or Gmail connector).
2. Name it `SendDiagnosticReport`. 
3. Description: *"Use this tool to email the final diagnostic report to the engineering team."*

## Step 6: Test and Run
1. Save the workflow.
2. Run a test by manually triggering it with a sample ADF error payload.
3. Open the **Run History** in the Logic App designer to observe the Agent Loop in action. You will see it:
   - Receive the error.
   - Decide to invoke `SearchKnowledgeBase`.
   - Process the JSON response from our Python API.
   - Formulate a human-readable diagnosis.
   - Decide to invoke `SendDiagnosticReport`.

By moving orchestration to Logic Apps, we gain high-availability enterprise orchestration while keeping the heavy lifting of vector search and knowledge base semantic matching inside our optimized Python backend!
