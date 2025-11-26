# Life of a [Component] Documentation Skill

This skill helps you create comprehensive "Life of a [X]" technical documents that trace the execution path through CockroachDB's codebase, following the pattern established by `docs/tech-notes/life_of_a_query.md`.

## When to Use This Skill

Use this skill when you need to document:
- The complete execution path of a major operation (e.g., query, transaction, schema change)
- How data flows through multiple system layers
- Integration between different subsystems
- End-to-end behavior for onboarding or understanding purposes

## Document Structure Template

### 1. Header Section

```markdown
# Life of a [Component/Operation Name]

Original author: [Name] (updated [Month Year])
```

### 2. Introduction Section

Include:
- **Purpose**: Clear statement of what this document aims to explain
- **Approach**: Explain that you'll trace code paths through various layers, providing high-level unifying view
- **Scope**: What's covered and what's intentionally omitted for brevity
- **Focus**: Note whether this covers actual code (descriptive) vs. design decisions (prescriptive)
- **Intended audience**: Who will benefit from this document (e.g., new engineers, contributors, curious readers)
- **Cross-references**: Links to related design docs, RFCs, or architecture overviews

Example format from life_of_a_query.md:
```markdown
## Introduction

This document aims to explain [the execution of X in CockroachDB],
following the code paths through the various layers of the system: [layer1],
[layer2], [layer3], etc. The idea is to provide a high-level unifying view
of the structure of the various components; none will be explored in particular
depth but pointers to other documentation will be provided where such
documentation exists. Code pointers will abound.

This document will generally not discuss design decisions, but rather focus on
tracing through the actual (current) code. In the interest of brevity, it only
covers the most significant and common parts of [X execution], and omits a
large amount of detail and special cases.

The intended audience is folks curious about a walk through the architecture of
a modern database, presented differently than in a design doc. It will hopefully
also be helpful for open source contributors and new Cockroach Labs engineers.
```

### 3. Main Content Sections

Organize sections by **layers/stages of execution** in chronological order:

#### Section Guidelines:

1. **Start at the entry point** (e.g., client connection, RPC call, API endpoint)
2. **Follow the data/request flow** through each major layer
3. **Use concrete examples** with actual code where helpful
4. **Include diagrams** when they clarify complex flows
5. **Provide code links** extensively - link to:
   - Package documentation
   - Key types/structs
   - Important methods/functions
   - Specific line numbers where critical operations occur

#### Section Pattern:

```markdown
## [Layer/Component Name]

[Brief description of this layer's role and responsibilities]

[Narrative explanation of what happens in this layer, with code references]

### [Sub-component if needed]

[Detailed walkthrough with code links]
```

### 4. Code Reference Format

Always provide GitHub permalinks (with specific commits) rather than relative paths:
- Good: `[pgwire.conn](https://github.com/cockroachdb/cockroach/blob/89621764d4c2d438d1781238f10e9ef27ef2c392/pkg/sql/pgwire/conn.go#L55)`
- Avoid: `pkg/sql/pgwire/conn.go`

Use inline links for flow: "It then [calls X](link) which [delegates to Y](link)"

### 5. Examples and Illustrations

Include:
- **SQL examples** with setup and output when documenting SQL paths
- **EXPLAIN output** to show query plans
- **Diagrams** (include both image and link to interactive version if available)
- **Protocol examples** for network/RPC interactions
- **Concrete values** (don't just say "a key", show `/companies/Apple`)

### 6. Handling Complexity

When encountering complex subsystems:
- Acknowledge the complexity
- Provide brief summary of key concepts
- Link to detailed documentation
- Use "we won't go into depth here" when appropriate
- Focus on the "happy path" and note where error handling/edge cases exist

Example:
```markdown
Query planning is a complex topic that could easily fill several articles
of its own, so we won't go into any depth here — see the
[`opt` package documentation](link) for further details.
```

### 7. Special Techniques

#### Code Path Tracing Language:
- Use active voice: "calls", "delegates", "returns", "iterates"
- Show the call chain: `A → B → C`
- Use verbs like "passes to", "routes through", "evaluates in"

#### Handling Recursion/Callbacks:
- Explicitly call out when recursion occurs
- Explain the base case or termination condition
- Use "Brace yourself:" or similar to flag complex flows

#### Managing Detail Level:
- Start broad, drill down progressively
- Use "Let's trace this deeper" or "Let's see what happens inside X"
- Summarize when backing out: "We've now seen how..."

## Process for Creating a Life-of Document

1. **Choose the Entry Point**: Identify where the operation starts (e.g., client request, internal trigger)

2. **Trace the Execution Path**:
   - Use a debugger or read through the code
   - Take notes on each major function call
   - Identify layer boundaries
   - Note important data transformations

3. **Identify Key Layers**: Group the execution into logical layers or stages

4. **Create Examples**: Develop concrete, runnable examples that demonstrate the flow

5. **Find Visual Aids**: Look for or create diagrams (especially for distributed operations)

6. **Write in Layers**:
   - Start with the introduction
   - Draft each section following execution order
   - Add code links as you go
   - Include examples in relevant sections

7. **Add Cross-References**: Link to design docs, RFCs, blog posts, and other tech notes

8. **Review for Completeness**:
   - Does it cover the main path end-to-end?
   - Are there enough code links?
   - Will a new engineer be able to follow this?
   - Are complex areas acknowledged with pointers to more info?

## Key Characteristics of Good Life-of Documents

✓ **Concrete**: Uses real code, real examples, real paths
✓ **Linear**: Follows execution chronologically
✓ **Connected**: Links extensively to actual code
✓ **Scoped**: Explicitly states what's out of scope
✓ **Practical**: Focuses on how things work, not why they were designed that way
✓ **Accessible**: Readable by someone unfamiliar with the codebase
✓ **Current**: Uses permalinks to specific commits; includes update date

## Common Patterns to Document

Good candidates for "Life of a..." documents:

- Life of a Transaction
- Life of a Schema Change
- Life of a Range Split
- Life of a Backup/Restore
- Life of a Lease Acquisition
- Life of a Replication Change
- Life of a SQL Type Conversion
- Life of a Gossip Message
- Life of a Raft Proposal

## Anti-Patterns to Avoid

✗ Design rationale without code paths (that's for RFCs)
✗ API documentation (that's for godoc)
✗ Exhaustive coverage of every edge case
✗ Tutorial or how-to guide (that's for docs.cockroachlabs.com)
✗ Broken or outdated code links
✗ Abstract descriptions without concrete examples

## Example Outline for "Life of a Schema Change"

```markdown
# Life of a Schema Change

Original author: [Name] (updated [Date])

## Introduction
[Purpose, scope, audience as per template above]

## Schema Change Request
[How DDL enters the system via pgwire]

## Statement Parsing and Planning
[How ALTER TABLE is parsed and planned]

## Declarative Schema Changer
[Overview and entry point]
### State Machine Design
### Plan Generation
### Job Creation

## Job Execution
[Jobs system integration]
### Backfilling
### Validation
### State Transitions

## Lease Management
[Schema lease interaction]

## Transaction Coordination
[How schema changes coordinate with ongoing transactions]

## Replication and Consensus
[How schema changes propagate across nodes]

## Completion and Cleanup
[Final steps and side effects]
```

## Usage Instructions

When asked to create a "Life of X" document:

1. Confirm the scope with the user
2. Identify the entry point for X
3. Trace through the codebase identifying major layers
4. Create a concrete example to follow
5. Write following the structure template above
6. Include extensive code links (with commit hashes)
7. Add diagrams or examples where helpful
8. Review for completeness and accuracy
