package org.heigit.bigspatialdata.oshdb.notes;

public enum NoteAction {
  OPENED("opened"),
  CLOSED("closed"),
  REOPENED("reopened"),
  COMMENTED("commented"),
  HIDDEN("hidden");

  private final String action;

  NoteAction(String action) {
    this.action = action;
  }

  public static NoteAction fromString(String text) {
    for (NoteAction b : NoteAction.values()) {
      if (b.action.equalsIgnoreCase(text)) {
        return b;
      }
    }
    throw new IllegalArgumentException("No constant with text " + text + " found");
  }

  @Override
  public String toString() {
    return action;
  }

}
