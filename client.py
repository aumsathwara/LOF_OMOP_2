import asyncio
import os
import sys
import json
from contextlib import AsyncExitStack
from typing import Optional
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

# Load Gemini API key
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY environment variable is required")
client = genai.Client(api_key=GEMINI_API_KEY)

class MCPClient:
    def __init__(self):
        self.session: Optional[ClientSession] = None
        self.exit_stack = AsyncExitStack()
        self.anthropic = client

    async def connect(self, server_script: str):
        # Spawn the MCP server in a child process
        command = sys.executable
        args = [server_script]
        params = StdioServerParameters(command=command, args=args, env=os.environ)
        transport = await self.exit_stack.enter_async_context(stdio_client(params))
        self.stdio, self.write = transport
        self.session = await self.exit_stack.enter_async_context(
            ClientSession(self.stdio, self.write)
        )
        await self.session.initialize()
        tools = await self.session.list_tools()
        print("Connected. Tools available:")
        for t in tools.tools:
            print(f" - {t.name}: {t.description}")

    async def process_query(self, query: str) -> dict:
        """Run a single query through Gemini + MCP tools, return text + usage."""
        # Build tool schema
        resp_tools = await self.session.list_tools()
        tool_descs = []
        for t in resp_tools.tools:
            tool_descs.append({
                "name": t.name,
                "description": t.description,
                "parameters": {
                    "type": t.inputSchema.get("type", "object"),
                    "properties": {
                        k: {"type": v.get("type", "string"), "description": v.get("description","")}
                        for k, v in t.inputSchema.get("properties", {}).items()
                    },
                    "required": t.inputSchema.get("required", [])
                }
            })
        tools_schema = types.Tool(function_declarations=tool_descs)
        config = types.GenerateContentConfig(tools=[tools_schema])
        
        # Call Gemini
        response = self.anthropic.models.generate_content(
            model="gemini-1.5-flash",
            contents=[query],
            config=config,
        )
    
        # Token usage
        um = response.usage_metadata
        prompt_tokens = um.prompt_token_count
        completion_tokens = um.candidates_token_count

        final_parts = []
        for cand in response.candidates:
            # If parts exist, iterate; else fallback to cand.content.text
            parts = getattr(cand.content, 'parts', None)
            if parts:
                for part in parts:
                    if hasattr(part, 'function_call') and part.function_call:
                        name = part.function_call.name
                        raw_args = part.function_call.args
                        args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                        print(f"\n[Calling tool {name} with args {args}]")
                        tool_response = await self.session.call_tool(name, args)
                        raw = getattr(tool_response.content[0], 'text', '')
                        try:
                            data = json.loads(raw)
                            list_result = "\n".join(item.get("text","") for item in data)
                        except:
                            list_result = raw
                        print(f"\nResults: {list_result}")
                    elif hasattr(part, 'text') and part.text:
                        final_parts.append(part.text)
            else:
                # Fallback for models without parts
                text_val = getattr(cand.content, 'text', '')
                if text_val:
                    final_parts.append(text_val)

        text = "\n".join(final_parts).strip()
        return {
            "text": text,
            "usage_metadata": {
                "prompt_token_count": prompt_tokens,
                "response_token_count": completion_tokens
            }
        }

    async def chat_loop(self):
        print("\nMCP Client Started!")
        print("Type your queries or 'quit' to exit.")
        while True:
            q = input("\nQuery: ").strip()
            if q.lower() in ('quit', 'exit'):
                break
            try:
                out = await self.process_query(q)
                text = out['text']
                um = out['usage_metadata']
                total = um['prompt_token_count'] + um['response_token_count']
                print(f"\n{text}")
                print(
                    f"\n[Tokens used: {total} "
                    f"(prompt {um['prompt_token_count']}, "
                    f"response {um['response_token_count']})]"
                )
            except Exception as e:
                print(f"\nError: {e}")

    async def cleanup(self):
        await self.exit_stack.aclose()

async def async_main(script_path: str):
     client = MCPClient()
     try:
         await client.connect(script_path)
         await client.chat_loop()
     finally:
         await client.cleanup()

def main():
     import sys
     if len(sys.argv) < 2:
         print("Usage: python client.py <path/to/server.py>")
         sys.exit(1)
     asyncio.run(async_main(sys.argv[1]))

if __name__ == "__main__":
     main()
