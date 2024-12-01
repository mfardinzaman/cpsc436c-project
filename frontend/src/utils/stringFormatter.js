const titleCaseToSentence = (str) => {
    return str
      .replace(/_/g, ' ')
      .toLowerCase()
      .replace(/\b\w/g, (char) => char.toUpperCase());
  };

export { titleCaseToSentence }